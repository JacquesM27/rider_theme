/*
Wymagane pakiety NuGet:
- RabbitMQ.Client
- Dapper
- Microsoft.Data.SqlClient
- Microsoft.Extensions.Hosting
- Microsoft.Extensions.Logging
- Microsoft.Extensions.DependencyInjection
*/

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Model konfiguracji kolejki
public class QueueConfig
{
    public string QueueName { get; set; }
    public bool Durable { get; set; } = true;
    public bool Exclusive { get; set; } = false;
    public bool AutoDelete { get; set; } = false;
    public ushort PrefetchCount { get; set; } = 10;
    public int RetryDelayMs { get; set; } = 5000;
    public Dictionary<string, object> Arguments { get; set; } = new();
}

// Interface dla ładowania konfiguracji
public interface IConfigLoader
{
    Task<QueueConfig> GetQueueConfigAsync(string queueName);
    Task<IEnumerable<string>> GetAllQueueNamesAsync();
}

// Implementacja config loader (przykład)
public class AppSettingsConfigLoader : IConfigLoader
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<AppSettingsConfigLoader> _logger;

    public AppSettingsConfigLoader(IConfiguration configuration, ILogger<AppSettingsConfigLoader> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public Task<QueueConfig> GetQueueConfigAsync(string queueName)
    {
        var section = _configuration.GetSection($"Queues:{queueName}");
        if (!section.Exists())
        {
            throw new InvalidOperationException($"Konfiguracja dla kolejki {queueName} nie została znaleziona");
        }

        var config = new QueueConfig
        {
            QueueName = queueName,
            Durable = section.GetValue("Durable", true),
            Exclusive = section.GetValue("Exclusive", false),
            AutoDelete = section.GetValue("AutoDelete", false),
            PrefetchCount = section.GetValue<ushort>("PrefetchCount", 10),
            RetryDelayMs = section.GetValue("RetryDelayMs", 5000)
        };

        return Task.FromResult(config);
    }

    public Task<IEnumerable<string>> GetAllQueueNamesAsync()
    {
        var queuesSection = _configuration.GetSection("Queues");
        var queueNames = queuesSection.GetChildren().Select(x => x.Key).ToList();
        
        _logger.LogInformation("Znaleziono kolejki w konfiguracji: {QueueNames}", string.Join(", ", queueNames));
        return Task.FromResult<IEnumerable<string>>(queueNames);
    }
}

// Ulepszone zarządzanie połączeniem z obsługą awarii
public interface IRabbitMqManager : IDisposable
{
    Task<IModel> GetChannelAsync(string queueName);
    Task ReleaseChannelAsync(string queueName);
    Task RecreateChannelAsync(string queueName);
    bool IsConnected { get; }
    event EventHandler<ConnectionEventArgs> ConnectionLost;
    event EventHandler<ConnectionEventArgs> ConnectionRestored;
}

public class ConnectionEventArgs : EventArgs
{
    public string Reason { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public class RabbitMqManager : IRabbitMqManager
{
    private readonly IConnectionFactory _connectionFactory;
    private readonly ILogger<RabbitMqManager> _logger;
    private readonly ConcurrentDictionary<string, ChannelWrapper> _channels;
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly Timer _healthCheckTimer;
    private IConnection _connection;
    private bool _disposed;
    private volatile bool _isReconnecting;

    public event EventHandler<ConnectionEventArgs> ConnectionLost;
    public event EventHandler<ConnectionEventArgs> ConnectionRestored;

    public RabbitMqManager(IConnectionFactory connectionFactory, ILogger<RabbitMqManager> logger)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _channels = new ConcurrentDictionary<string, ChannelWrapper>();
        _connectionSemaphore = new SemaphoreSlim(1, 1);
        
        // Health check co 30 sekund
        _healthCheckTimer = new Timer(CheckConnectionHealth, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
    }

    public bool IsConnected => _connection?.IsOpen == true;

    public async Task<IModel> GetChannelAsync(string queueName)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqManager));

        await EnsureConnectionAsync();

        var channelWrapper = _channels.GetOrAdd(queueName, key => new ChannelWrapper(key));
        
        if (channelWrapper.Channel?.IsOpen != true)
        {
            await channelWrapper.Semaphore.WaitAsync();
            try
            {
                if (channelWrapper.Channel?.IsOpen != true)
                {
                    await CreateChannelAsync(channelWrapper, queueName);
                }
            }
            finally
            {
                channelWrapper.Semaphore.Release();
            }
        }

        return channelWrapper.Channel;
    }

    public async Task ReleaseChannelAsync(string queueName)
    {
        if (_channels.TryRemove(queueName, out var channelWrapper))
        {
            await channelWrapper.Semaphore.WaitAsync();
            try
            {
                channelWrapper.Channel?.Close();
                channelWrapper.Channel?.Dispose();
                _logger.LogInformation("Channel dla kolejki {QueueName} został zwolniony", queueName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Błąd podczas zwalniania kanału dla kolejki {QueueName}", queueName);
            }
            finally
            {
                channelWrapper.Semaphore.Release();
                channelWrapper.Dispose();
            }
        }
    }

    public async Task RecreateChannelAsync(string queueName)
    {
        if (_channels.TryGetValue(queueName, out var channelWrapper))
        {
            await channelWrapper.Semaphore.WaitAsync();
            try
            {
                // Zamknij stary kanał
                try
                {
                    channelWrapper.Channel?.Close();
                    channelWrapper.Channel?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Błąd podczas zamykania starego kanału dla kolejki {QueueName}", queueName);
                }

                // Upewnij się że connection jest dostępne
                await EnsureConnectionAsync();
                
                // Utwórz nowy kanał
                await CreateChannelAsync(channelWrapper, queueName);
                _logger.LogInformation("Kanał dla kolejki {QueueName} został odtworzony", queueName);
            }
            finally
            {
                channelWrapper.Semaphore.Release();
            }
        }
    }

    private async Task EnsureConnectionAsync()
    {
        if (_connection?.IsOpen == true)
            return;

        await _connectionSemaphore.WaitAsync();
        try
        {
            if (_connection?.IsOpen == true)
                return;

            _isReconnecting = true;
            _connection?.Dispose();
            
            var retryCount = 0;
            const int maxRetries = 5;
            
            while (retryCount < maxRetries)
            {
                try
                {
                    _connection = await Task.Run(() => _connectionFactory.CreateConnection());
                    
                    _connection.ConnectionShutdown += OnConnectionShutdown;
                    _connection.CallbackException += OnCallbackException;
                    
                    _logger.LogInformation("Połączenie z RabbitMQ zostało nawiązane");
                    
                    if (retryCount > 0)
                    {
                        ConnectionRestored?.Invoke(this, new ConnectionEventArgs { Reason = "Connection restored after failure" });
                    }
                    
                    _isReconnecting = false;
                    return;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    _logger.LogError(ex, "Próba {RetryCount}/{MaxRetries} nawiązania połączenia z RabbitMQ nie powiodła się", retryCount, maxRetries);
                    
                    if (retryCount < maxRetries)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryCount))); // Exponential backoff
                    }
                }
            }
            
            _isReconnecting = false;
            throw new InvalidOperationException($"Nie udało się nawiązać połączenia z RabbitMQ po {maxRetries} próbach");
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    private async Task CreateChannelAsync(ChannelWrapper channelWrapper, string queueName)
    {
        try
        {
            channelWrapper.Channel = _connection.CreateModel();
            
            channelWrapper.Channel.ModelShutdown += (sender, args) =>
            {
                _logger.LogWarning("Kanał dla kolejki {QueueName} został zamknięty: {Reason}", 
                    queueName, args.ReplyText);
            };

            _logger.LogDebug("Utworzono kanał dla kolejki {QueueName}", queueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas tworzenia kanału dla kolejki {QueueName}", queueName);
            throw;
        }
    }

    private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
    {
        _logger.LogWarning("Połączenie RabbitMQ zostało zamknięte: {Reason}", e.ReplyText);
        
        ConnectionLost?.Invoke(this, new ConnectionEventArgs { Reason = e.ReplyText });
        
        // Oznacz wszystkie kanały jako nieważne
        foreach (var channel in _channels.Values)
        {
            try
            {
                channel.Channel?.Close();
            }
            catch { }
        }
    }

    private void OnCallbackException(object sender, CallbackExceptionEventArgs e)
    {
        _logger.LogError(e.Exception, "Wystąpił wyjątek w callback RabbitMQ");
    }

    private async void CheckConnectionHealth(object state)
    {
        if (_disposed || _isReconnecting)
            return;

        try
        {
            if (!IsConnected)
            {
                _logger.LogWarning("Health check wykrył utratę połączenia - próba ponownego nawiązania");
                await EnsureConnectionAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas health check połączenia");
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _healthCheckTimer?.Dispose();

        foreach (var kvp in _channels)
        {
            try
            {
                kvp.Value.Channel?.Close();
                kvp.Value.Channel?.Dispose();
                kvp.Value.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Błąd podczas zwalniania kanału {QueueName}", kvp.Key);
            }
        }

        _channels.Clear();

        try
        {
            _connection?.Close();
            _connection?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Błąd podczas zamykania połączenia RabbitMQ");
        }

        _connectionSemaphore?.Dispose();
    }

    private class ChannelWrapper : IDisposable
    {
        public string QueueName { get; }
        public IModel Channel { get; set; }
        public SemaphoreSlim Semaphore { get; }

        public ChannelWrapper(string queueName)
        {
            QueueName = queueName;
            Semaphore = new SemaphoreSlim(1, 1);
        }

        public void Dispose()
        {
            Semaphore?.Dispose();
        }
    }
}

// Model dla wiadomości w bazie danych
public class ProcessedMessage
{
    public string MessageId { get; set; }
    public string QueueName { get; set; }
    public string Content { get; set; }
    public DateTime ProcessedAt { get; set; }
    public string Status { get; set; }
}

// Repository interfaces i implementacje (bez zmian)
public interface IMessageRepository
{
    Task<bool> IsMessageProcessedAsync(string messageId, IDbTransaction transaction = null);
    Task SaveMessageAsync(ProcessedMessage message, IDbTransaction transaction);
    Task UpdateMessageStatusAsync(string messageId, string status, IDbTransaction transaction);
}

public class MessageRepository : IMessageRepository
{
    private readonly string _connectionString;
    private readonly ILogger<MessageRepository> _logger;

    public MessageRepository(string connectionString, ILogger<MessageRepository> logger)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<bool> IsMessageProcessedAsync(string messageId, IDbTransaction transaction = null)
    {
        const string sql = @"
            SELECT COUNT(1) 
            FROM ProcessedMessages 
            WHERE MessageId = @MessageId AND Status IN ('Completed', 'Processing')";

        try
        {
            IDbConnection connection = transaction?.Connection ?? new SqlConnection(_connectionString);
            
            if (transaction == null)
            {
                using (connection)
                {
                    var count = await connection.QuerySingleAsync<int>(sql, new { MessageId = messageId });
                    return count > 0;
                }
            }
            else
            {
                var count = await connection.QuerySingleAsync<int>(sql, new { MessageId = messageId }, transaction);
                return count > 0;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas sprawdzania czy wiadomość {MessageId} została przetworzona", messageId);
            throw;
        }
    }

    public async Task SaveMessageAsync(ProcessedMessage message, IDbTransaction transaction)
    {
        const string sql = @"
            INSERT INTO ProcessedMessages (MessageId, QueueName, Content, ProcessedAt, Status)
            VALUES (@MessageId, @QueueName, @Content, @ProcessedAt, @Status)";

        try
        {
            var affected = await transaction.Connection.ExecuteAsync(sql, message, transaction);
            
            if (affected == 0)
            {
                throw new InvalidOperationException($"Nie udało się zapisać wiadomości {message.MessageId}");
            }
            
            _logger.LogDebug("Zapisano wiadomość {MessageId} do bazy danych", message.MessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas zapisywania wiadomości {MessageId}", message.MessageId);
            throw;
        }
    }

    public async Task UpdateMessageStatusAsync(string messageId, string status, IDbTransaction transaction)
    {
        const string sql = @"
            UPDATE ProcessedMessages 
            SET Status = @Status, ProcessedAt = @ProcessedAt
            WHERE MessageId = @MessageId";

        try
        {
            var affected = await transaction.Connection.ExecuteAsync(sql, 
                new { MessageId = messageId, Status = status, ProcessedAt = DateTime.UtcNow }, 
                transaction);
                
            if (affected == 0)
            {
                _logger.LogWarning("Nie znaleziono wiadomości {MessageId} do aktualizacji", messageId);
            }
            
            _logger.LogDebug("Zaktualizowano status wiadomości {MessageId} na {Status}", messageId, status);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas aktualizacji statusu wiadomości {MessageId}", messageId);
            throw;
        }
    }
}

public interface ITransactionService
{
    Task<T> ExecuteInTransactionAsync<T>(Func<IDbTransaction, Task<T>> operation);
    Task ExecuteInTransactionAsync(Func<IDbTransaction, Task> operation);
}

public class TransactionService : ITransactionService
{
    private readonly string _connectionString;
    private readonly ILogger<TransactionService> _logger;

    public TransactionService(string connectionString, ILogger<TransactionService> logger)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<T> ExecuteInTransactionAsync<T>(Func<IDbTransaction, Task<T>> operation)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            var result = await operation(transaction);
            transaction.Commit();
            return result;
        }
        catch (Exception ex)
        {
            try
            {
                transaction.Rollback();
                _logger.LogWarning("Transakcja została wycofana z powodu błędu: {Error}", ex.Message);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogError(rollbackEx, "Błąd podczas wycofywania transakcji");
            }
            throw;
        }
    }

    public async Task ExecuteInTransactionAsync(Func<IDbTransaction, Task> operation)
    {
        await ExecuteInTransactionAsync(async transaction =>
        {
            await operation(transaction);
            return true;
        });
    }
}

// Oddzielny QueueConsumerService dla każdej kolejki
public class QueueConsumerService : BackgroundService
{
    private readonly string _queueName;
    private readonly IRabbitMqManager _rabbitMqManager;
    private readonly IConfigLoader _configLoader;
    private readonly IMessageRepository _messageRepository;
    private readonly ITransactionService _transactionService;
    private readonly ILogger<QueueConsumerService> _logger;
    private readonly IServiceProvider _serviceProvider;
    
    private QueueConfig _queueConfig;
    private volatile bool _isRestarting;

    public QueueConsumerService(
        string queueName,
        IRabbitMqManager rabbitMqManager,
        IConfigLoader configLoader,
        IMessageRepository messageRepository,
        ITransactionService transactionService,
        ILogger<QueueConsumerService> logger,
        IServiceProvider serviceProvider)
    {
        _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
        _rabbitMqManager = rabbitMqManager ?? throw new ArgumentNullException(nameof(rabbitMqManager));
        _configLoader = configLoader ?? throw new ArgumentNullException(nameof(configLoader));
        _messageRepository = messageRepository ?? throw new ArgumentNullException(nameof(messageRepository));
        _transactionService = transactionService ?? throw new ArgumentNullException(nameof(transactionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));

        // Subskrybuj eventy connection manager
        _rabbitMqManager.ConnectionLost += OnConnectionLost;
        _rabbitMqManager.ConnectionRestored += OnConnectionRestored;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Uruchamianie consumer dla kolejki {QueueName}", _queueName);
        
        try
        {
            _queueConfig = await _configLoader.GetQueueConfigAsync(_queueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Nie udało się załadować konfiguracji dla kolejki {QueueName}", _queueName);
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeMessages(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Consumer dla kolejki {QueueName} został zatrzymany", _queueName);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Błąd w consumer dla kolejki {QueueName} - restart za {DelayMs}ms", 
                    _queueName, _queueConfig.RetryDelayMs);
                
                try
                {
                    await Task.Delay(_queueConfig.RetryDelayMs, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task ConsumeMessages(CancellationToken cancellationToken)
    {
        IModel channel = null;
        EventingBasicConsumer consumer = null;
        string consumerTag = null;

        try
        {
            // Pobierz kanał
            channel = await _rabbitMqManager.GetChannelAsync(_queueName);
            
            // Skonfiguruj kolejkę
            channel.QueueDeclare(
                queue: _queueConfig.QueueName,
                durable: _queueConfig.Durable,
                exclusive: _queueConfig.Exclusive,
                autoDelete: _queueConfig.AutoDelete,
                arguments: _queueConfig.Arguments);

            // Ustaw QoS
            channel.BasicQos(0, _queueConfig.PrefetchCount, false);

            // Utwórz consumer
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                await HandleMessage(ea, channel);
            };

            consumer.Shutdown += (sender, args) =>
            {
                _logger.LogWarning("Consumer dla kolejki {QueueName} został zamknięty: {Reason}", 
                    _queueName, args.ReplyText);
            };

            // Rozpocznij konsumpcję
            consumerTag = channel.BasicConsume(
                queue: _queueConfig.QueueName,
                autoAck: false,
                consumer: consumer);

            _logger.LogInformation("Consumer dla kolejki {QueueName} został uruchomiony z tagiem {ConsumerTag}", 
                _queueName, consumerTag);

            // Czekaj na zamknięcie
            while (!cancellationToken.IsCancellationRequested && 
                   channel.IsOpen && 
                   _rabbitMqManager.IsConnected &&
                   !_isRestarting)
            {
                await Task.Delay(1000, cancellationToken);
            }

            if (_isRestarting)
            {
                _logger.LogInformation("Restart consumer dla kolejki {QueueName}", _queueName);
            }
        }
        finally
        {
            // Cleanup
            try
            {
                if (consumerTag != null && channel?.IsOpen == true)
                {
                    channel.BasicCancel(consumerTag);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Błąd podczas anulowania consumer dla kolejki {QueueName}", _queueName);
            }
        }
    }

    private async Task HandleMessage(BasicDeliverEventArgs ea, IModel channel)
    {
        var messageId = ea.BasicProperties?.MessageId ?? Guid.NewGuid().ToString();
        var correlationId = ea.BasicProperties?.CorrelationId ?? "unknown";
        
        _logger.LogDebug("Otrzymano wiadomość {MessageId} z kolejki {QueueName}", 
            messageId, _queueName);

        try
        {
            await ProcessMessageWithTransaction(ea, messageId);
            channel.BasicAck(ea.DeliveryTag, false);
            _logger.LogDebug("Pomyślnie przetworzona wiadomość {MessageId}", messageId);
        }
        catch (DuplicateMessageException)
        {
            channel.BasicAck(ea.DeliveryTag, false);
            _logger.LogInformation("Wiadomość {MessageId} została już wcześniej przetworzona", messageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd podczas przetwarzania wiadomości {MessageId} z kolejki {QueueName}", 
                messageId, _queueName);
            
            channel.BasicNack(ea.DeliveryTag, false, true);
            
            try
            {
                await _transactionService.ExecuteInTransactionAsync(async transaction =>
                {
                    await _messageRepository.UpdateMessageStatusAsync(messageId, "Failed", transaction);
                });
            }
            catch (Exception updateEx)
            {
                _logger.LogError(updateEx, "Nie udało się zaktualizować statusu wiadomości {MessageId} na Failed", messageId);
            }
        }
    }

    private async Task ProcessMessageWithTransaction(BasicDeliverEventArgs ea, string messageId)
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
                Status = "Processing"
            };

            await _messageRepository.SaveMessageAsync(processedMessage, transaction);

            // Przetworz wiadomość
            await ProcessBusinessLogic(messageContent, messageId, transaction);

            await _messageRepository.UpdateMessageStatusAsync(messageId, "Completed", transaction);
        });
    }

    private async Task ProcessBusinessLogic(string messageContent, string messageId, IDbTransaction transaction)
    {
        _logger.LogInformation("Przetwarzam wiadomość {MessageId} z kolejki {QueueName}: {Content}", 
            messageId, _queueName, messageContent);

        // Twoja logika biznesowa tutaj
        await Task.Delay(100);
        
        if (messageContent.Contains("ERROR"))
        {
            throw new InvalidOperationException("Symulowany błąd przetwarzania");
        }
    }

    private void OnConnectionLost(object sender, ConnectionEventArgs e)
    {
        _logger.LogWarning("Utracono połączenie dla consumer kolejki {QueueName}: {Reason}", _queueName, e.Reason);
        _isRestarting = true;
    }

    private void OnConnectionRestored(object sender, ConnectionEventArgs e)
    {
        _logger.LogInformation("Przywrócono połączenie dla consumer kolejki {QueueName}: {Reason}", _queueName, e.Reason);
        _isRestarting = false;
    }

    public override void Dispose()
    {
        _rabbitMqManager.ConnectionLost -= OnConnectionLost;
        _rabbitMqManager.ConnectionRestored -= OnConnectionRestored;
        base.Dispose();
    }
}

// Wyjątek dla duplikatów wiadomości
public class DuplicateMessageException : Exception
{
    public DuplicateMessageException(string message) : base(message) { }
}

// Factory dla tworzenia QueueConsumerService
public interface IQueueConsumerFactory
{
    QueueConsumerService CreateConsumer(string queueName);
}

public class QueueConsumerFactory : IQueueConsumerFactory
{
    private readonly IServiceProvider _serviceProvider;

    public QueueConsumerFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public QueueConsumerService CreateConsumer(string queueName)
    {
        return new QueueConsumerService(
            queueName,
            _serviceProvider.GetRequiredService<IRabbitMqManager>(),
            _serviceProvider.GetRequiredService<IConfigLoader>(),
            _serviceProvider.GetRequiredService<IMessageRepository>(),
            _serviceProvider.GetRequiredService<ITransactionService>(),
            _serviceProvider.GetRequiredService<ILogger<QueueConsumerService>>(),
            _serviceProvider);
    }
}

// Serwis do zarządzania wszystkimi consumer
public class QueueConsumerManager : BackgroundService
{
    private readonly IConfigLoader _configLoader;
    private readonly IQueueConsumerFactory _consumerFactory;
    private readonly ILogger<QueueConsumerManager> _logger;
    private readonly List<QueueConsumerService> _consumers = new();

    public QueueConsumerManager(
        IConfigLoader configLoader,
        IQueueConsumerFactory consumerFactory,
        ILogger<QueueConsumerManager> logger)
    {
        _configLoader = configLoader;
        _consumerFactory = consumerFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var queueNames = await _configLoader.GetAllQueueNamesAsync();
            
            foreach (var queueName in queueNames)
            {
                var consumer = _consumerFactory.CreateConsumer(queueName);
                _consumers.Add(consumer);
                
                // Uruchom każdy consumer w osobnym wątku
                _ = Task.Run(() => consumer.StartAsync(stoppingToken), stoppingToken);
                
                _logger.LogInformation("Uruchomiono consumer dla kolejki {QueueName}", queueName);
            }

            // Czekaj na zakończenie
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(5000, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd w QueueConsumerManager");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Zatrzymywanie wszystkich consumer...");
        
        var stopTasks = _consumers.Select(consumer => consumer.StopAsync(cancellationToken));
        await Task.WhenAll(stopTasks);
        
        foreach (var consumer in _consumers)
        {
            consumer.Dispose();
        }
        
        _consumers.Clear();
        
        await base.StopAsync(cancellationToken);
    }
}

// Rozszerzenia dla Dependency Injection
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, 
        Action<ConnectionFactory> configureConnection)
    {
        services.AddSingleton<IConnectionFactory>(provider =>
        {
            var factory = new ConnectionFactory();
            configureConnection(factory);
            return factory;
        });

        services.AddSingleton<IRabbitMqManager, RabbitMqManager>();
        
        return services;
    }

    public static IServiceCollection AddMessageProcessing(this IServiceCollection services, 
        string connectionString)
    {
        services.AddScoped<IMessageRepository>(provider => 
            new MessageRepository(connectionString, provider.GetRequiredService<ILogger<MessageRepository>>()));
        
        services.AddScoped<ITransactionService>(provider => 
            new TransactionService(connectionString, provider.GetRequiredService<ILogger<TransactionService>>()));

        services.AddSingleton<IConfigLoader, AppSettingsConfigLoader>();
        services.AddSingleton<IQueueConsumerFactory, QueueConsumerFactory>();
        services.AddHostedService<QueueConsumerManager>();
        
        return services;
    }
}

// Konfiguracja w Program.cs
public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        var sqlConnectionString = builder.Configuration.GetConnectionString("DefaultConnection");

        // Konfiguracja RabbitMQ
        builder.Services.AddRabbitMq(factory =>
        {
            factory.HostName = builder.Configuration.GetValue("RabbitMQ:HostName", "localhost");
            factory.Port = builder.Configuration.GetValue("RabbitMQ:Port", 5672);
            factory.UserName = builder.Configuration.GetValue("RabbitMQ:UserName", "guest");
            factory.Password = builder.Configuration.GetValue("RabbitMQ:Password", "guest");
            factory.VirtualHost = builder.Configuration.GetValue("RabbitMQ:VirtualHost", "/");
            
            factory.AutomaticRecoveryEnabled = true;
            factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);
            factory.RequestedHeartbeat = TimeSpan.FromSeconds(60);
        });

        // Konfiguracja przetwarzania wiadomości
        builder.Services.AddMessageProcessing(sqlConnectionString);

        var app = builder.Build();
        
        // Tworzenie tabel w bazie danych przy starcie
        await CreateDatabaseTablesAsync(sqlConnectionString);
        
        app.Run();
    }

    private static async Task CreateDatabaseTablesAsync(string connectionString)
    {
        const string createTableSql = @"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ProcessedMessages' AND xtype='U')
            CREATE TABLE ProcessedMessages (
                Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                MessageId NVARCHAR(255) NOT NULL,
                QueueName NVARCHAR(255) NOT NULL,
                Content NVARCHAR(MAX),
                ProcessedAt DATETIME2 NOT NULL,
                Status NVARCHAR(50) NOT NULL,
                CONSTRAINT UK_ProcessedMessages_MessageId UNIQUE (MessageId)
            );
            
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_ProcessedMessages_MessageId_Status')
            CREATE INDEX IX_ProcessedMessages_MessageId_Status ON ProcessedMessages (MessageId, Status);
            
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_ProcessedMessages_QueueName_ProcessedAt')
            CREATE INDEX IX_ProcessedMessages_QueueName_ProcessedAt ON ProcessedMessages (QueueName, ProcessedAt);";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await connection.ExecuteAsync(createTableSql);
    }
}

/*
Przykład appsettings.json:
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=MessageProcessingDB;Trusted_Connection=true;TrustServerCertificate=true;"
  },
  "RabbitMQ": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/"
  },
  "Queues": {
    "orders": {
      "Durable": true,
      "PrefetchCount": 5,
      "RetryDelayMs": 3000
    },
    "notifications": {
      "Durable": true,
      "PrefetchCount": 20,
      "RetryDelayMs": 1000
    },
    "emails": {
      "Durable": true,
      "PrefetchCount": 10,
      "RetryDelayMs": 5000
    }
  }
}
*/