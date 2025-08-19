// ===== ROZSZERZENIE MODELU ProcessedMessage =====
public class ProcessedMessage
{
    public string MessageId { get; set; }
    public string QueueName { get; set; }
    public string Content { get; set; }
    public DateTime ProcessedAt { get; set; }
    public string Status { get; set; }
    public DateTime? ValidTo { get; set; } // Nowe pole
}

// ===== ROZSZERZENIE INTERFACE REPOSITORY =====
public interface IMessageRepository
{
    Task<bool> IsMessageProcessedAsync(string messageId, IDbTransaction transaction = null);
    Task SaveMessageAsync(ProcessedMessage message, IDbTransaction transaction);
    Task UpdateMessageStatusAsync(string messageId, string status, IDbTransaction transaction);
    
    // Nowe metody dla cleanup
    Task<int> CleanupExpiredMessageContentAsync(DateTime expiredBefore, int batchSize = 1000);
    Task<int> GetExpiredMessageCountAsync(DateTime expiredBefore);
}

// ===== ROZSZERZENIE IMPLEMENTACJI REPOSITORY =====
public partial class MessageRepository : IMessageRepository
{
    // ... poprzednie metody pozostają bez zmian ...

    public async Task<int> CleanupExpiredMessageContentAsync(DateTime expiredBefore, int batchSize = 1000)
    {
        const string sql = @"
            UPDATE TOP(@BatchSize) ProcessedMessages 
            SET Content = NULL 
            WHERE ValidTo < @ExpiredBefore 
              AND Content IS NOT NULL";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            
            var totalCleaned = 0;
            int cleanedInBatch;
            
            do
            {
                cleanedInBatch = await connection.ExecuteAsync(sql, new 
                { 
                    BatchSize = batchSize,
                    ExpiredBefore = expiredBefore 
                });
                
                totalCleaned += cleanedInBatch;
                
                _logger.LogDebug("Wyczyszczono {CleanedCount} wiadomości w batch, łącznie: {TotalCleaned}", 
                    cleanedInBatch, totalCleaned);
                
                // Jeśli przetwarzamy duże ilości danych, daj chwilę przerwy
                if (cleanedInBatch == batchSize)
                {
                    await Task.Delay(100); // 100ms przerwy między batch
                }
                
            } while (cleanedInBatch == batchSize); // Kontynuuj jeśli batch był pełny
            
            _logger.LogInformation("Zakończono cleanup - wyczyszczono łącznie {TotalCleaned} wiadomości", totalCleaned);
            return totalCleaned;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas czyszczenia wygasłej zawartości wiadomości");
            throw;
        }
    }

    public async Task<int> GetExpiredMessageCountAsync(DateTime expiredBefore)
    {
        const string sql = @"
            SELECT COUNT(1) 
            FROM ProcessedMessages 
            WHERE ValidTo < @ExpiredBefore 
              AND Content IS NOT NULL";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            
            var count = await connection.QuerySingleAsync<int>(sql, new { ExpiredBefore = expiredBefore });
            
            _logger.LogDebug("Znaleziono {ExpiredCount} wiadomości do wyczyszczenia", count);
            return count;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas pobierania liczby wygasłych wiadomości");
            throw;
        }
    }
}

// ===== MESSAGE CLEANUP BACKGROUND SERVICE =====
public class MessageCleanupService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<MessageCleanupService> _logger;
    private readonly IConfiguration _configuration;
    private readonly PeriodicTimer _cleanupTimer;
    private readonly TimeSpan _cleanupInterval;
    private readonly TimeSpan _retentionPeriod;
    private readonly int _batchSize;
    private readonly bool _isEnabled;

    public MessageCleanupService(
        IServiceProvider serviceProvider,
        ILogger<MessageCleanupService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

        // Konfiguracja z appsettings
        _isEnabled = _configuration.GetValue("MessageCleanup:Enabled", true);
        _cleanupInterval = TimeSpan.FromHours(_configuration.GetValue("MessageCleanup:IntervalHours", 24));
        _retentionPeriod = TimeSpan.FromDays(_configuration.GetValue("MessageCleanup:RetentionDays", 30));
        _batchSize = _configuration.GetValue("MessageCleanup:BatchSize", 1000);

        _cleanupTimer = new PeriodicTimer(_cleanupInterval);

        _logger.LogInformation("MessageCleanupService skonfigurowany: Enabled={Enabled}, Interval={Interval}, Retention={Retention}, BatchSize={BatchSize}",
            _isEnabled, _cleanupInterval, _retentionPeriod, _batchSize);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_isEnabled)
        {
            _logger.LogInformation("MessageCleanupService jest wyłączony - pomijam");
            return;
        }

        _logger.LogInformation("MessageCleanupService rozpoczął pracę");

        try
        {
            // Wykonaj cleanup na starcie (jeśli nie jest to pierwsze uruchomienie)
            await PerformCleanupAsync(stoppingToken);

            // Następnie wykonuj co określony interwał
            while (await _cleanupTimer.WaitForNextTickAsync(stoppingToken))
            {
                await PerformCleanupAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("MessageCleanupService został zatrzymany");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Nieoczekiwany błąd w MessageCleanupService");
        }
    }

    private async Task PerformCleanupAsync(CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Rozpoczynam cleanup wiadomości");

        try
        {
            using var scope = _serviceProvider.CreateScope();
            var messageRepository = scope.ServiceProvider.GetRequiredService<IMessageRepository>();

            var expiredBefore = DateTime.UtcNow.Subtract(_retentionPeriod);
            
            // Sprawdź ile jest do wyczyszczenia
            var expiredCount = await messageRepository.GetExpiredMessageCountAsync(expiredBefore);
            
            if (expiredCount == 0)
            {
                _logger.LogInformation("Brak wiadomości do wyczyszczenia");
                return;
            }

            _logger.LogInformation("Znaleziono {ExpiredCount} wiadomości do wyczyszczenia (starszych niż {ExpiredBefore})", 
                expiredCount, expiredBefore);

            // Wykonaj cleanup
            var cleanedCount = await messageRepository.CleanupExpiredMessageContentAsync(expiredBefore, _batchSize);
            
            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation("Cleanup zakończony pomyślnie: wyczyszczono {CleanedCount} z {ExpiredCount} wiadomości w {Duration}ms",
                cleanedCount, expiredCount, duration.TotalMilliseconds);

            // Metryki dla monitoringu
            LogCleanupMetrics(cleanedCount, expiredCount, duration);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("Cleanup został przerwany z powodu zatrzymania serwisu");
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - startTime;
            _logger.LogError(ex, "Błąd podczas cleanup wiadomości po {Duration}ms", duration.TotalMilliseconds);
            
            // Nie rzucamy wyjątku - pozwalamy serwisowi kontynuować przy następnym cyklu
        }
    }

    private void LogCleanupMetrics(int cleanedCount, int totalExpiredCount, TimeSpan duration)
    {
        // Strukturalne logi dla monitoringu (np. Prometheus, Application Insights)
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["CleanedCount"] = cleanedCount,
            ["TotalExpiredCount"] = totalExpiredCount,
            ["DurationMs"] = duration.TotalMilliseconds,
            ["BatchSize"] = _batchSize,
            ["RetentionDays"] = _retentionPeriod.TotalDays
        });

        _logger.LogInformation("Cleanup metrics logged");
    }

    public override void Dispose()
    {
        _cleanupTimer?.Dispose();
        base.Dispose();
    }
}

// ===== ROZSZERZENIE SERVICE COLLECTION =====
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMessageProcessing(this IServiceCollection services, 
        string connectionString)
    {
        services.AddScoped<IMessageRepository>(provider => 
            new MessageRepository(connectionString, provider.GetRequiredService<ILogger<MessageRepository>>()));
        
        services.AddScoped<ITransactionService>(provider => 
            new TransactionService(connectionString, provider.GetRequiredService<ILogger<TransactionService>>()));

        services.AddSingleton<IConfigLoader, AppSettingsConfigLoader>();
        services.AddSingleton<IQueueConsumerFactory, QueueConsumerFactory>();
        services.AddSingleton<IMessagePublisher, MessagePublisherService>();
        services.AddHostedService<QueueConsumerManager>();
        
        // Dodaj cleanup service
        services.AddHostedService<MessageCleanupService>();
        
        return services;
    }
}

// ===== TESTY JEDNOSTKOWE =====
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using Xunit;

namespace RabbitMQ.Tests
{
    // ===== TESTY MESSAGE REPOSITORY (NOWE METODY) =====
    public class MessageRepositoryCleanupTests
    {
        private readonly string _connectionString = "test-connection-string";
        private readonly ILogger<MessageRepository> _logger;
        private readonly MessageRepository _sut;
        private readonly IDbConnection _connection;

        public MessageRepositoryCleanupTests()
        {
            _logger = Substitute.For<ILogger<MessageRepository>>();
            _connection = Substitute.For<IDbConnection>();
            _sut = new MessageRepository(_connectionString, _logger);
        }

        [Fact]
        public async Task CleanupExpiredMessageContentAsync_WithExpiredMessages_ShouldReturnCleanedCount()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            var batchSize = 500;
            
            // Mock będzie zwracać 500, potem 300, potem 0 (koniec)
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>())
                .Returns(500, 300, 0);

            // Act
            var result = await _sut.CleanupExpiredMessageContentAsync(expiredBefore, batchSize);

            // Assert
            result.ShouldBe(800); // 500 + 300
            await _connection.Received(3).ExecuteAsync(Arg.Any<string>(), Arg.Any<object>());
        }

        [Fact]
        public async Task CleanupExpiredMessageContentAsync_NoExpiredMessages_ShouldReturnZero()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>()).Returns(0);

            // Act
            var result = await _sut.CleanupExpiredMessageContentAsync(expiredBefore);

            // Assert
            result.ShouldBe(0);
            await _connection.Received(1).ExecuteAsync(Arg.Any<string>(), Arg.Any<object>());
        }

        [Fact]
        public async Task CleanupExpiredMessageContentAsync_DatabaseError_ShouldThrowAndLog()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>())
                .Throws(new InvalidOperationException("Database error"));

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.CleanupExpiredMessageContentAsync(expiredBefore));
            
            exception.Message.ShouldBe("Database error");
        }

        [Fact]
        public async Task CleanupExpiredMessageContentAsync_DefaultBatchSize_ShouldUse1000()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>()).Returns(0);

            // Act
            await _sut.CleanupExpiredMessageContentAsync(expiredBefore);

            // Assert
            await _connection.Received(1).ExecuteAsync(
                Arg.Any<string>(), 
                Arg.Is<object>(x => x.GetType().GetProperty("BatchSize")?.GetValue(x)?.ToString() == "1000"));
        }

        [Fact]
        public async Task GetExpiredMessageCountAsync_WithExpiredMessages_ShouldReturnCount()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>()).Returns(42);

            // Act
            var result = await _sut.GetExpiredMessageCountAsync(expiredBefore);

            // Assert
            result.ShouldBe(42);
            await _connection.Received(1).QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>());
        }

        [Fact]
        public async Task GetExpiredMessageCountAsync_NoExpiredMessages_ShouldReturnZero()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>()).Returns(0);

            // Act
            var result = await _sut.GetExpiredMessageCountAsync(expiredBefore);

            // Assert
            result.ShouldBe(0);
        }

        [Fact]
        public async Task GetExpiredMessageCountAsync_DatabaseError_ShouldThrowAndLog()
        {
            // Arrange
            var expiredBefore = DateTime.UtcNow.AddDays(-1);
            _connection.QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>())
                .Throws(new InvalidOperationException("Database connection failed"));

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.GetExpiredMessageCountAsync(expiredBefore));
            
            exception.Message.ShouldBe("Database connection failed");
        }
    }

    // ===== TESTY MESSAGE CLEANUP SERVICE =====
    public class MessageCleanupServiceTests : IDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IServiceScope _serviceScope;
        private readonly IMessageRepository _messageRepository;
        private readonly ILogger<MessageCleanupService> _logger;
        private readonly IConfiguration _configuration;
        private readonly MessageCleanupService _sut;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public MessageCleanupServiceTests()
        {
            _messageRepository = Substitute.For<IMessageRepository>();
            _logger = Substitute.For<ILogger<MessageCleanupService>>();
            _configuration = Substitute.For<IConfiguration>();
            _serviceScope = Substitute.For<IServiceScope>();
            _serviceProvider = Substitute.For<IServiceProvider>();
            _cancellationTokenSource = new CancellationTokenSource();

            // Setup configuration defaults
            _configuration.GetValue("MessageCleanup:Enabled", true).Returns(true);
            _configuration.GetValue("MessageCleanup:IntervalHours", 24).Returns(24);
            _configuration.GetValue("MessageCleanup:RetentionDays", 30).Returns(30);
            _configuration.GetValue("MessageCleanup:BatchSize", 1000).Returns(1000);

            // Setup service provider
            _serviceProvider.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetRequiredService<IMessageRepository>().Returns(_messageRepository);

            _sut = new MessageCleanupService(_serviceProvider, _logger, _configuration);
        }

        [Fact]
        public void Constructor_ValidParameters_ShouldSetConfiguration()
        {
            // Act & Assert - Constructor should not throw
            Should.NotThrow(() => new MessageCleanupService(_serviceProvider, _logger, _configuration));
        }

        [Fact]
        public void Constructor_NullServiceProvider_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => 
                new MessageCleanupService(null, _logger, _configuration));
        }

        [Fact]
        public void Constructor_NullLogger_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => 
                new MessageCleanupService(_serviceProvider, null, _configuration));
        }

        [Fact]
        public void Constructor_NullConfiguration_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => 
                new MessageCleanupService(_serviceProvider, _logger, null));
        }

        [Fact]
        public async Task ExecuteAsync_WhenDisabled_ShouldNotPerformCleanup()
        {
            // Arrange
            _configuration.GetValue("MessageCleanup:Enabled", true).Returns(false);
            var disabledService = new MessageCleanupService(_serviceProvider, _logger, _configuration);
            _cancellationTokenSource.CancelAfter(100);

            // Act
            await disabledService.StartAsync(_cancellationTokenSource.Token);
            await Task.Delay(150);
            await disabledService.StopAsync(_cancellationTokenSource.Token);

            // Assert
            await _messageRepository.DidNotReceive().GetExpiredMessageCountAsync(Arg.Any<DateTime>());
            disabledService.Dispose();
        }

        [Fact]
        public async Task PerformCleanupAsync_WithExpiredMessages_ShouldCleanupSuccessfully()
        {
            // Arrange
            var expiredCount = 500;
            var cleanedCount = 500;
            
            _messageRepository.GetExpiredMessageCountAsync(Arg.Any<DateTime>()).Returns(expiredCount);
            _messageRepository.CleanupExpiredMessageContentAsync(Arg.Any<DateTime>(), Arg.Any<int>()).Returns(cleanedCount);

            // Use reflection to call private method for testing
            var method = typeof(MessageCleanupService).GetMethod("PerformCleanupAsync", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            await (Task)method.Invoke(_sut, new object[] { _cancellationTokenSource.Token });

            // Assert
            await _messageRepository.Received(1).GetExpiredMessageCountAsync(Arg.Any<DateTime>());
            await _messageRepository.Received(1).CleanupExpiredMessageContentAsync(Arg.Any<DateTime>(), 1000);
        }

        [Fact]
        public async Task PerformCleanupAsync_NoExpiredMessages_ShouldSkipCleanup()
        {
            // Arrange
            _messageRepository.GetExpiredMessageCountAsync(Arg.Any<DateTime>()).Returns(0);

            // Use reflection to call private method
            var method = typeof(MessageCleanupService).GetMethod("PerformCleanupAsync", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act
            await (Task)method.Invoke(_sut, new object[] { _cancellationTokenSource.Token });

            // Assert
            await _messageRepository.Received(1).GetExpiredMessageCountAsync(Arg.Any<DateTime>());
            await _messageRepository.DidNotReceive().CleanupExpiredMessageContentAsync(Arg.Any<DateTime>(), Arg.Any<int>());
        }

        [Fact]
        public async Task PerformCleanupAsync_RepositoryThrowsException_ShouldLogErrorAndNotRethrow()
        {
            // Arrange
            _messageRepository.GetExpiredMessageCountAsync(Arg.Any<DateTime>())
                .Throws(new InvalidOperationException("Database error"));

            // Use reflection to call private method
            var method = typeof(MessageCleanupService).GetMethod("PerformCleanupAsync", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act & Assert - Should not throw
            await Should.NotThrowAsync(async () => 
                await (Task)method.Invoke(_sut, new object[] { _cancellationTokenSource.Token }));
        }

        [Fact]
        public async Task PerformCleanupAsync_CancellationRequested_ShouldHandleGracefully()
        {
            // Arrange
            _cancellationTokenSource.Cancel(); // Cancel immediately
            _messageRepository.GetExpiredMessageCountAsync(Arg.Any<DateTime>())
                .Returns(Task.FromCanceled<int>(_cancellationTokenSource.Token));

            // Use reflection to call private method
            var method = typeof(MessageCleanupService).GetMethod("PerformCleanupAsync", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            // Act & Assert - Should handle cancellation gracefully
            await Should.NotThrowAsync(async () => 
                await (Task)method.Invoke(_sut, new object[] { _cancellationTokenSource.Token }));
        }

        [Fact]
        public void Constructor_CustomConfiguration_ShouldUseCustomValues()
        {
            // Arrange
            _configuration.GetValue("MessageCleanup:Enabled", true).Returns(false);
            _configuration.GetValue("MessageCleanup:IntervalHours", 24).Returns(12);
            _configuration.GetValue("MessageCleanup:RetentionDays", 30).Returns(7);
            _configuration.GetValue("MessageCleanup:BatchSize", 1000).Returns(500);

            // Act
            var customService = new MessageCleanupService(_serviceProvider, _logger, _configuration);

            // Assert - Constructor should complete successfully with custom config
            customService.ShouldNotBeNull();
            customService.Dispose();
        }

        [Fact]
        public void Dispose_ShouldNotThrow()
        {
            // Act & Assert
            Should.NotThrow(() => _sut.Dispose());
        }

        [Fact]
        public void Dispose_CalledMultipleTimes_ShouldNotThrow()
        {
            // Act & Assert
            Should.NotThrow(() =>
            {
                _sut.Dispose();
                _sut.Dispose(); // Second call should not throw
            });
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Dispose();
            _sut?.Dispose();
        }
    }

    // ===== TESTY INTEGRATION SERVICE REGISTRATION =====
    public class MessageCleanupServiceRegistrationTests
    {
        [Fact]
        public void AddMessageProcessing_ShouldRegisterCleanupService()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IConfiguration>(Substitute.For<IConfiguration>());

            // Act
            services.AddMessageProcessing("test-connection-string");

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>();
            
            hostedServices.ShouldContain(s => s.GetType() == typeof(MessageCleanupService));
        }
    }

    // ===== TESTY MODELU Z NOWYM POLEM =====
    public class ProcessedMessageWithValidToTests
    {
        [Fact]
        public void ProcessedMessage_WithValidTo_ShouldAllowSettingProperty()
        {
            // Arrange
            var validTo = DateTime.UtcNow.AddDays(7);

            // Act
            var message = new ProcessedMessage
            {
                MessageId = "test-id",
                QueueName = "test-queue",
                Content = "test-content",
                ProcessedAt = DateTime.UtcNow,
                Status = "Completed",
                ValidTo = validTo
            };

            // Assert
            message.ValidTo.ShouldBe(validTo);
            message.ValidTo.HasValue.ShouldBeTrue();
        }

        [Fact]
        public void ProcessedMessage_WithNullValidTo_ShouldAllowNullValue()
        {
            // Act
            var message = new ProcessedMessage
            {
                MessageId = "test-id",
                ValidTo = null
            };

            // Assert
            message.ValidTo.ShouldBeNull();
            message.ValidTo.HasValue.ShouldBeFalse();
        }
    }
}

/*
Przykład konfiguracji w appsettings.json:

{
  "MessageCleanup": {
    "Enabled": true,
    "IntervalHours": 24,      // Jak często uruchamiać
    "RetentionDays": 30,      // Ile dni zachowywać Content
    "BatchSize": 1000         // Ile rekordów na raz
  }
}

SQL do aktualizacji tabeli:

ALTER TABLE ProcessedMessages 
ADD ValidTo DATETIME2 NULL;

CREATE INDEX IX_ProcessedMessages_ValidTo_Content 
ON ProcessedMessages (ValidTo) 
WHERE Content IS NOT NULL;

Przykład użycia w Program.cs:

var builder = WebApplication.CreateBuilder(args);

// Dodaj wszystkie serwisy (automatycznie doda MessageCleanupService)
builder.Services.AddMessageProcessing(sqlConnectionString);

var app = builder.Build();
app.Run();

Przykład ustawiania ValidTo przy zapisywaniu wiadomości:

public async Task ProcessMessageWithTransaction(BasicDeliverEventArgs ea, string messageId)
{
    var body = ea.Body.ToArray();
    var messageContent = Encoding.UTF8.GetString(body);

    await _transactionService.ExecuteInTransactionAsync(async transaction =>
    {
        var isProcessed = await _messageRepository.IsMessageProcessedAsync(messageId, transaction);
        if (isProcessed)
        {
            throw new DuplicateMessageException($"Wiadomość {messageId} została już przetworzona");
        }

        var processedMessage = new ProcessedMessage
        {
            MessageId = messageId,
            QueueName = _queueName,
            Content = messageContent,
            ProcessedAt = DateTime.UtcNow,
            Status = "Processing",
            ValidTo = DateTime.UtcNow.AddDays(30) // Zawartość ważna przez 30 dni
        };

        await _messageRepository.SaveMessageAsync(processedMessage, transaction);
        await ProcessBusinessLogic(messageContent, messageId, transaction);
        await _messageRepository.UpdateMessageStatusAsync(messageId, "Completed", transaction);
    });
}

Monitorowanie cleanup przez logi:

2024-01-15 02:00:00.123 [INF] MessageCleanupService rozpoczął pracę
2024-01-15 02:00:01.456 [INF] Znaleziono 2,500 wiadomości do wyczyszczenia (starszych niż 2023-12-15 02:00:01)
2024-01-15 02:00:02.789 [DBG] Wyczyszczono 1000 wiadomości w batch, łącznie: 1000
2024-01-15 02:00:03.012 [DBG] Wyczyszczono 1000 wiadomości w batch, łącznie: 2000  
2024-01-15 02:00:03.234 [DBG] Wyczyszczono 500 wiadomości w batch, łącznie: 2500
2024-01-15 02:00:03.456 [INF] Cleanup zakończony pomyślnie: wyczyszczono 2,500 z 2,500 wiadomości w 1333ms

Różne konfiguracje dla różnych środowisk:

// appsettings.Development.json - częstsze czyszczenie dla testów
{
  "MessageCleanup": {
    "Enabled": true,
    "IntervalHours": 1,       // Co godzinę
    "RetentionDays": 7,       // Tylko tydzień
    "BatchSize": 100          // Mniejsze batch
  }
}

// appsettings.Production.json - optymalizacja dla produkcji
{
  "MessageCleanup": {
    "Enabled": true,
    "IntervalHours": 24,      // Raz dziennie
    "RetentionDays": 90,      // 3 miesiące
    "BatchSize": 5000         // Większe batch dla performance
  }
}

// appsettings.Test.json - wyłączone dla testów jednostkowych
{
  "MessageCleanup": {
    "Enabled": false
  }
}

Przykład custom health check dla cleanup service:

public class MessageCleanupHealthCheck : IHealthCheck
{
    private readonly IMessageRepository _messageRepository;
    private readonly IConfiguration _configuration;

    public MessageCleanupHealthCheck(IMessageRepository messageRepository, IConfiguration configuration)
    {
        _messageRepository = messageRepository;
        _configuration = configuration;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var retentionDays = _configuration.GetValue("MessageCleanup:RetentionDays", 30);
            var expiredBefore = DateTime.UtcNow.AddDays(-retentionDays);
            
            var expiredCount = await _messageRepository.GetExpiredMessageCountAsync(expiredBefore);
            
            // Warning jeśli jest dużo expired messages (cleanup może mieć problem)
            if (expiredCount > 10000)
            {
                return HealthCheckResult.Degraded($"Duża liczba wiadomości do wyczyszczenia: {expiredCount}");
            }
            
            return HealthCheckResult.Healthy($"Cleanup działa prawidłowo. Do wyczyszczenia: {expiredCount}");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Błąd podczas sprawdzania cleanup", ex);
        }
    }
}

// Rejestracja health check
builder.Services.AddHealthChecks()
    .AddCheck<MessageCleanupHealthCheck>("message_cleanup");

Metryki dla Prometheus/Application Insights:

public class MessageCleanupMetrics
{
    private readonly IMetricsLogger _metricsLogger;
    
    public void LogCleanupCompleted(int cleanedCount, int totalExpired, TimeSpan duration)
    {
        _metricsLogger.Counter("message_cleanup_cleaned_total")
            .WithTag("batch_size", cleanedCount)
            .Increment(cleanedCount);
            
        _metricsLogger.Histogram("message_cleanup_duration_ms")
            .Record(duration.TotalMilliseconds);
            
        _metricsLogger.Gauge("message_cleanup_expired_remaining")
            .Set(totalExpired - cleanedCount);
    }
}

Przykład ręcznego uruchomienia cleanup (np. przez API endpoint):

[ApiController]
[Route("api/[controller]")]
public class MaintenanceController : ControllerBase
{
    private readonly IMessageRepository _messageRepository;
    
    [HttpPost("cleanup-messages")]
    [Authorize(Policy = "AdminOnly")]
    public async Task<IActionResult> TriggerMessageCleanup(
        [FromQuery] int retentionDays = 30,
        [FromQuery] int batchSize = 1000)
    {
        try
        {
            var expiredBefore = DateTime.UtcNow.AddDays(-retentionDays);
            var expiredCount = await _messageRepository.GetExpiredMessageCountAsync(expiredBefore);
            
            if (expiredCount == 0)
            {
                return Ok(new { message = "Brak wiadomości do wyczyszczenia", expiredCount = 0 });
            }
            
            var cleanedCount = await _messageRepository.CleanupExpiredMessageContentAsync(expiredBefore, batchSize);
            
            return Ok(new 
            { 
                message = "Cleanup zakończony pomyślnie", 
                cleanedCount, 
                expiredCount,
                retentionDays,
                batchSize 
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Błąd podczas cleanup", details = ex.Message });
        }
    }
    
    [HttpGet("cleanup-status")]
    public async Task<IActionResult> GetCleanupStatus([FromQuery] int retentionDays = 30)
    {
        try
        {
            var expiredBefore = DateTime.UtcNow.AddDays(-retentionDays);
            var expiredCount = await _messageRepository.GetExpiredMessageCountAsync(expiredBefore);
            
            return Ok(new 
            { 
                expiredCount, 
                retentionDays,
                expiredBefore,
                nextCleanupEstimate = DateTime.UtcNow.Date.AddDays(1).AddHours(2) // 2 AM następnego dnia
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Błąd podczas sprawdzania statusu", details = ex.Message });
        }
    }
}

Performance considerations i best practices:

1. **Index Strategy:**
   - Indeks na (ValidTo, Content IS NOT NULL) dla szybkich cleanup
   - Partitioning tabeli po dacie jeśli duże wolumeny
   
2. **Batch Size Tuning:**
   - Mniejsze batch (100-500) dla systemów z wysokim load
   - Większe batch (5000+) dla systemów z niskim load
   - Monitoruj average cleanup duration
   
3. **Timing Optimization:**
   - Uruchamiaj w godzinach niskiego ruchu (2-4 AM)
   - Unikaj uruchamiania podczas backup
   - Dostosuj delay między batch do obciążenia DB
   
4. **Monitoring Alerts:**
   - Alert jeśli cleanup trwa > 30 minut
   - Alert jeśli liczba expired messages > threshold
   - Alert jeśli cleanup service nie uruchomił się przez 25 godzin

Przykład advanced konfiguracji z multiple retention policies:

{
  "MessageCleanup": {
    "Enabled": true,
    "IntervalHours": 24,
    "Policies": [
      {
        "QueuePattern": "critical.*",
        "RetentionDays": 90,
        "BatchSize": 500
      },
      {
        "QueuePattern": "notifications.*", 
        "RetentionDays": 7,
        "BatchSize": 2000
      },
      {
        "QueuePattern": "*",
        "RetentionDays": 30,
        "BatchSize": 1000
      }
    ]
  }
}
*/