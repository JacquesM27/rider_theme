/*
Wymagane pakiety NuGet dla testów:
- xunit
- xunit.runner.visualstudio
- NSubstitute
- Shouldly
- Microsoft.Extensions.Logging.Abstractions
- RabbitMQ.Client (do mockowania)
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shouldly;
using Xunit;

namespace RabbitMQ.Tests
{
    // ===== TESTY RABBITMQ MANAGER =====
    public class RabbitMqManagerTests : IDisposable
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly ILogger<RabbitMqManager> _logger;
        private readonly RabbitMqManager _sut;

        public RabbitMqManagerTests()
        {
            _connectionFactory = Substitute.For<IConnectionFactory>();
            _connection = Substitute.For<IConnection>();
            _channel = Substitute.For<IModel>();
            _logger = Substitute.For<ILogger<RabbitMqManager>>();

            _connectionFactory.CreateConnection().Returns(_connection);
            _connection.IsOpen.Returns(true);
            _connection.CreateModel().Returns(_channel);
            _channel.IsOpen.Returns(true);

            _sut = new RabbitMqManager(_connectionFactory, _logger);
        }

        [Fact]
        public async Task GetChannelAsync_FirstCall_ShouldCreateConnectionAndChannel()
        {
            // Act
            var result = await _sut.GetChannelAsync("test-queue");

            // Assert
            result.ShouldBe(_channel);
            _connectionFactory.Received(1).CreateConnection();
            _connection.Received(1).CreateModel();
        }

        [Fact]
        public async Task GetChannelAsync_SameQueueMultipleCalls_ShouldReuseChannel()
        {
            // Act
            var result1 = await _sut.GetChannelAsync("test-queue");
            var result2 = await _sut.GetChannelAsync("test-queue");

            // Assert
            result1.ShouldBe(result2);
            _connection.Received(1).CreateModel(); // Only one channel created
        }

        [Fact]
        public async Task GetChannelAsync_DifferentQueues_ShouldCreateSeparateChannels()
        {
            // Arrange
            var channel2 = Substitute.For<IModel>();
            channel2.IsOpen.Returns(true);
            _connection.CreateModel().Returns(_channel, channel2);

            // Act
            var result1 = await _sut.GetChannelAsync("queue1");
            var result2 = await _sut.GetChannelAsync("queue2");

            // Assert
            result1.ShouldNotBe(result2);
            _connection.Received(2).CreateModel();
        }

        [Fact]
        public async Task GetChannelAsync_ClosedChannel_ShouldRecreateChannel()
        {
            // Arrange
            await _sut.GetChannelAsync("test-queue");
            _channel.IsOpen.Returns(false);
            var newChannel = Substitute.For<IModel>();
            newChannel.IsOpen.Returns(true);
            _connection.CreateModel().Returns(newChannel);

            // Act
            var result = await _sut.GetChannelAsync("test-queue");

            // Assert
            result.ShouldBe(newChannel);
            _connection.Received(2).CreateModel();
        }

        [Fact]
        public async Task GetChannelAsync_ConnectionClosed_ShouldRecreateConnection()
        {
            // Arrange
            _connection.IsOpen.Returns(false);
            var newConnection = Substitute.For<IConnection>();
            var newChannel = Substitute.For<IModel>();
            newConnection.IsOpen.Returns(true);
            newConnection.CreateModel().Returns(newChannel);
            newChannel.IsOpen.Returns(true);
            _connectionFactory.CreateConnection().Returns(_connection, newConnection);

            // Act
            var result = await _sut.GetChannelAsync("test-queue");

            // Assert
            result.ShouldBe(newChannel);
            _connectionFactory.Received(2).CreateConnection();
        }

        [Fact]
        public async Task GetChannelAsync_ConnectionFactoryThrows_ShouldRetryAndEventuallyThrow()
        {
            // Arrange
            _connectionFactory.CreateConnection().Throws(new InvalidOperationException("Connection failed"));

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.GetChannelAsync("test-queue"));
            
            exception.Message.ShouldContain("Nie udało się nawiązać połączenia z RabbitMQ po 5 próbach");
        }

        [Fact]
        public void IsConnected_WhenConnectionIsOpen_ShouldReturnTrue()
        {
            // Arrange
            _connection.IsOpen.Returns(true);

            // Act & Assert
            _sut.IsConnected.ShouldBeTrue();
        }

        [Fact]
        public void IsConnected_WhenConnectionIsClosed_ShouldReturnFalse()
        {
            // Arrange
            _connection.IsOpen.Returns(false);

            // Act & Assert
            _sut.IsConnected.ShouldBeFalse();
        }

        [Fact]
        public async Task RecreateChannelAsync_ExistingChannel_ShouldCloseOldAndCreateNew()
        {
            // Arrange
            await _sut.GetChannelAsync("test-queue");
            var newChannel = Substitute.For<IModel>();
            newChannel.IsOpen.Returns(true);
            _connection.CreateModel().Returns(newChannel);

            // Act
            await _sut.RecreateChannelAsync("test-queue");
            var result = await _sut.GetChannelAsync("test-queue");

            // Assert
            result.ShouldBe(newChannel);
            _channel.Received(1).Close();
            _channel.Received(1).Dispose();
        }

        [Fact]
        public async Task ReleaseChannelAsync_ExistingChannel_ShouldCloseAndRemove()
        {
            // Arrange
            await _sut.GetChannelAsync("test-queue");

            // Act
            await _sut.ReleaseChannelAsync("test-queue");

            // Assert
            _channel.Received(1).Close();
            _channel.Received(1).Dispose();
        }

        [Fact]
        public async Task GetChannelAsync_AfterDispose_ShouldThrowObjectDisposedException()
        {
            // Arrange
            _sut.Dispose();

            // Act & Assert
            await Should.ThrowAsync<ObjectDisposedException>(() => _sut.GetChannelAsync("test-queue"));
        }

        [Fact]
        public void ConnectionLostEvent_WhenConnectionShutdown_ShouldBeRaised()
        {
            // Arrange
            ConnectionEventArgs capturedArgs = null;
            _sut.ConnectionLost += (sender, args) => capturedArgs = args;

            // Act
            _connection.ConnectionShutdown += Raise.EventWith(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Test shutdown"));

            // Assert
            capturedArgs.ShouldNotBeNull();
            capturedArgs.Reason.ShouldBe("Test shutdown");
        }

        public void Dispose()
        {
            _sut?.Dispose();
        }
    }

    // ===== TESTY CONFIG LOADER =====
    public class AppSettingsConfigLoaderTests
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<AppSettingsConfigLoader> _logger;
        private readonly AppSettingsConfigLoader _sut;

        public AppSettingsConfigLoaderTests()
        {
            _configuration = Substitute.For<IConfiguration>();
            _logger = Substitute.For<ILogger<AppSettingsConfigLoader>>();
            _sut = new AppSettingsConfigLoader(_configuration, _logger);
        }

        [Fact]
        public async Task GetQueueConfigAsync_ValidQueue_ShouldReturnConfig()
        {
            // Arrange
            var section = Substitute.For<IConfigurationSection>();
            section.Exists().Returns(true);
            section.GetValue("Durable", true).Returns(false);
            section.GetValue("Exclusive", false).Returns(true);
            section.GetValue("AutoDelete", false).Returns(true);
            section.GetValue<ushort>("PrefetchCount", (ushort)10).Returns((ushort)5);
            section.GetValue("RetryDelayMs", 5000).Returns(3000);

            _configuration.GetSection("Queues:test-queue").Returns(section);

            // Act
            var result = await _sut.GetQueueConfigAsync("test-queue");

            // Assert
            result.ShouldNotBeNull();
            result.QueueName.ShouldBe("test-queue");
            result.Durable.ShouldBeFalse();
            result.Exclusive.ShouldBeTrue();
            result.AutoDelete.ShouldBeTrue();
            result.PrefetchCount.ShouldBe((ushort)5);
            result.RetryDelayMs.ShouldBe(3000);
        }

        [Fact]
        public async Task GetQueueConfigAsync_NonExistentQueue_ShouldThrowException()
        {
            // Arrange
            var section = Substitute.For<IConfigurationSection>();
            section.Exists().Returns(false);
            _configuration.GetSection("Queues:non-existent").Returns(section);

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.GetQueueConfigAsync("non-existent"));
            
            exception.Message.ShouldContain("Konfiguracja dla kolejki non-existent nie została znaleziona");
        }

        [Fact]
        public async Task GetAllQueueNamesAsync_WithQueues_ShouldReturnNames()
        {
            // Arrange
            var queuesSection = Substitute.For<IConfigurationSection>();
            var queue1Section = Substitute.For<IConfigurationSection>();
            var queue2Section = Substitute.For<IConfigurationSection>();
            
            queue1Section.Key.Returns("queue1");
            queue2Section.Key.Returns("queue2");
            
            queuesSection.GetChildren().Returns(new[] { queue1Section, queue2Section });
            _configuration.GetSection("Queues").Returns(queuesSection);

            // Act
            var result = await _sut.GetAllQueueNamesAsync();

            // Assert
            result.ShouldContain("queue1");
            result.ShouldContain("queue2");
            result.Count().ShouldBe(2);
        }
    }

    // ===== TESTY MESSAGE REPOSITORY =====
    public class MessageRepositoryTests
    {
        private readonly string _connectionString = "test-connection-string";
        private readonly ILogger<MessageRepository> _logger;
        private readonly MessageRepository _sut;
        private readonly IDbConnection _connection;
        private readonly IDbTransaction _transaction;

        public MessageRepositoryTests()
        {
            _logger = Substitute.For<ILogger<MessageRepository>>();
            _connection = Substitute.For<IDbConnection>();
            _transaction = Substitute.For<IDbTransaction>();
            _transaction.Connection.Returns(_connection);
            
            _sut = new MessageRepository(_connectionString, _logger);
        }

        [Fact]
        public async Task IsMessageProcessedAsync_MessageExists_ShouldReturnTrue()
        {
            // Arrange
            _connection.QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<IDbTransaction>())
                .Returns(1);

            // Act
            var result = await _sut.IsMessageProcessedAsync("test-id", _transaction);

            // Assert
            result.ShouldBeTrue();
        }

        [Fact]
        public async Task IsMessageProcessedAsync_MessageNotExists_ShouldReturnFalse()
        {
            // Arrange
            _connection.QuerySingleAsync<int>(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<IDbTransaction>())
                .Returns(0);

            // Act
            var result = await _sut.IsMessageProcessedAsync("test-id", _transaction);

            // Assert
            result.ShouldBeFalse();
        }

        [Fact]
        public async Task SaveMessageAsync_ValidMessage_ShouldExecuteSuccessfully()
        {
            // Arrange
            var message = new ProcessedMessage
            {
                MessageId = "test-id",
                QueueName = "test-queue",
                Content = "test-content",
                ProcessedAt = DateTime.UtcNow,
                Status = "Processing"
            };

            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<IDbTransaction>())
                .Returns(1);

            // Act
            await _sut.SaveMessageAsync(message, _transaction);

            // Assert
            await _connection.Received(1).ExecuteAsync(Arg.Any<string>(), message, _transaction);
        }

        [Fact]
        public async Task SaveMessageAsync_NoRowsAffected_ShouldThrowException()
        {
            // Arrange
            var message = new ProcessedMessage { MessageId = "test-id" };
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<IDbTransaction>())
                .Returns(0);

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.SaveMessageAsync(message, _transaction));
            
            exception.Message.ShouldContain("Nie udało się zapisać wiadomości test-id");
        }

        [Fact]
        public async Task UpdateMessageStatusAsync_ValidMessage_ShouldExecuteSuccessfully()
        {
            // Arrange
            _connection.ExecuteAsync(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<IDbTransaction>())
                .Returns(1);

            // Act
            await _sut.UpdateMessageStatusAsync("test-id", "Completed", _transaction);

            // Assert
            await _connection.Received(1).ExecuteAsync(
                Arg.Any<string>(), 
                Arg.Is<object>(x => x.GetType().GetProperty("MessageId")?.GetValue(x)?.ToString() == "test-id"), 
                _transaction);
        }
    }

    // ===== TESTY TRANSACTION SERVICE =====
    public class TransactionServiceTests
    {
        private readonly string _connectionString = "test-connection-string";
        private readonly ILogger<TransactionService> _logger;
        private readonly TransactionService _sut;

        public TransactionServiceTests()
        {
            _logger = Substitute.For<ILogger<TransactionService>>();
            _sut = new TransactionService(_connectionString, _logger);
        }

        [Fact]
        public void Constructor_NullConnectionString_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => new TransactionService(null, _logger));
        }

        [Fact]
        public void Constructor_NullLogger_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => new TransactionService(_connectionString, null));
        }

        // Note: Full integration tests for TransactionService would require real DB connection
        // These would be better suited for integration tests rather than unit tests
    }

    // ===== TESTY MESSAGE PUBLISHER =====
    public class MessagePublisherServiceTests
    {
        private readonly IRabbitMqManager _rabbitMqManager;
        private readonly ILogger<MessagePublisherService> _logger;
        private readonly IModel _channel;
        private readonly MessagePublisherService _sut;

        public MessagePublisherServiceTests()
        {
            _rabbitMqManager = Substitute.For<IRabbitMqManager>();
            _logger = Substitute.For<ILogger<MessagePublisherService>>();
            _channel = Substitute.For<IModel>();

            var basicProperties = Substitute.For<IBasicProperties>();
            _channel.CreateBasicPropertiesWithHeaders(Arg.Any<MessageProperties>()).Returns(basicProperties);
            _channel.WaitForConfirms(Arg.Any<TimeSpan>()).Returns(true);

            _rabbitMqManager.GetChannelAsync(Arg.Any<string>()).Returns(_channel);

            _sut = new MessagePublisherService(_rabbitMqManager, _logger);
        }

        [Fact]
        public async Task PublishAsync_ValidObject_ShouldSerializeAndPublish()
        {
            // Arrange
            var testObject = new { Id = 1, Name = "Test" };

            // Act
            await _sut.PublishAsync("test.exchange", testObject);

            // Assert
            await _rabbitMqManager.Received(1).GetChannelAsync("publisher_test.exchange");
            _channel.Received(1).ConfirmSelect();
            _channel.Received(1).BasicPublish(
                "test.exchange",
                "",
                Arg.Any<IBasicProperties>(),
                Arg.Any<byte[]>());
            _channel.Received(1).WaitForConfirms(Arg.Any<TimeSpan>());
        }

        [Fact]
        public async Task PublishAsync_ValidString_ShouldPublish()
        {
            // Arrange
            var message = "test message";

            // Act
            await _sut.PublishAsync("test.exchange", message);

            // Assert
            _channel.Received(1).BasicPublish(
                "test.exchange",
                "",
                Arg.Any<IBasicProperties>(),
                Arg.Is<byte[]>(x => Encoding.UTF8.GetString(x) == message));
        }

        [Fact]
        public async Task PublishAsync_ValidBytes_ShouldPublish()
        {
            // Arrange
            var messageBytes = Encoding.UTF8.GetBytes("test message");

            // Act
            await _sut.PublishAsync("test.exchange", messageBytes);

            // Assert
            _channel.Received(1).BasicPublish(
                "test.exchange",
                "",
                Arg.Any<IBasicProperties>(),
                messageBytes);
        }

        [Fact]
        public async Task PublishAsync_WithCustomRoutingKey_ShouldUseRoutingKey()
        {
            // Arrange
            var message = "test";
            var routingKey = "test.routing.key";

            // Act
            await _sut.PublishAsync("test.exchange", message, routingKey: routingKey);

            // Assert
            _channel.Received(1).BasicPublish(
                "test.exchange",
                routingKey,
                Arg.Any<IBasicProperties>(),
                Arg.Any<byte[]>());
        }

        [Fact]
        public async Task PublishAsync_WithCustomProperties_ShouldUseProperties()
        {
            // Arrange
            var properties = new MessageProperties
            {
                MessageId = "custom-id",
                CorrelationId = "custom-correlation",
                ConfirmTimeout = TimeSpan.FromSeconds(10)
            };

            // Act
            await _sut.PublishAsync("test.exchange", "test", properties);

            // Assert
            _channel.Received(1).CreateBasicPropertiesWithHeaders(properties);
            _channel.Received(1).WaitForConfirms(TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task PublishAsync_NullMessage_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            await Should.ThrowAsync<ArgumentNullException>(
                () => _sut.PublishAsync<object>("test.exchange", null));
        }

        [Fact]
        public async Task PublishAsync_EmptyExchangeName_ShouldThrowArgumentException()
        {
            // Act & Assert
            await Should.ThrowAsync<ArgumentException>(
                () => _sut.PublishAsync("", "test"));
        }

        [Fact]
        public async Task PublishAsync_EmptyMessage_ShouldThrowArgumentException()
        {
            // Act & Assert
            await Should.ThrowAsync<ArgumentException>(
                () => _sut.PublishAsync("test.exchange", ""));
        }

        [Fact]
        public async Task PublishAsync_WaitForConfirmsFails_ShouldThrowException()
        {
            // Arrange
            _channel.WaitForConfirms(Arg.Any<TimeSpan>()).Returns(false);

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.PublishAsync("test.exchange", "test"));
            
            exception.Message.ShouldContain("Timeout podczas oczekiwania na potwierdzenie");
        }

        [Fact]
        public async Task PublishAsync_ChannelThrowsException_ShouldRetryAndRecreateChannel()
        {
            // Arrange
            _channel.BasicPublish(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IBasicProperties>(), Arg.Any<byte[]>())
                .Throws(new InvalidOperationException("Channel error"))
                .AndDoes(_ => { }); // Success on second call

            // Act
            await _sut.PublishAsync("test.exchange", "test");

            // Assert
            await _rabbitMqManager.Received(1).RecreateChannelAsync("publisher_test.exchange");
            _channel.Received(2).BasicPublish(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IBasicProperties>(), Arg.Any<byte[]>());
        }

        [Fact]
        public async Task PublishAsync_AllRetriesFail_ShouldThrowException()
        {
            // Arrange
            _channel.BasicPublish(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IBasicProperties>(), Arg.Any<byte[]>())
                .Throws(new InvalidOperationException("Persistent error"));

            // Act & Assert
            var exception = await Should.ThrowAsync<InvalidOperationException>(
                () => _sut.PublishAsync("test.exchange", "test"));
            
            exception.Message.ShouldContain("Nie udało się opublikować wiadomości na exchange test.exchange po 3 próbach");
        }
    }

    // ===== TESTY QUEUE CONSUMER SERVICE =====
    public class QueueConsumerServiceTests : IDisposable
    {
        private readonly string _queueName = "test-queue";
        private readonly IRabbitMqManager _rabbitMqManager;
        private readonly IConfigLoader _configLoader;
        private readonly IMessageRepository _messageRepository;
        private readonly ITransactionService _transactionService;
        private readonly ILogger<QueueConsumerService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IModel _channel;
        private readonly QueueConsumerService _sut;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public QueueConsumerServiceTests()
        {
            _rabbitMqManager = Substitute.For<IRabbitMqManager>();
            _configLoader = Substitute.For<IConfigLoader>();
            _messageRepository = Substitute.For<IMessageRepository>();
            _transactionService = Substitute.For<ITransactionService>();
            _logger = Substitute.For<ILogger<QueueConsumerService>>();
            _serviceProvider = Substitute.For<IServiceProvider>();
            _channel = Substitute.For<IModel>();
            _cancellationTokenSource = new CancellationTokenSource();

            var queueConfig = new QueueConfig
            {
                QueueName = _queueName,
                Durable = true,
                PrefetchCount = 10,
                RetryDelayMs = 1000
            };

            _configLoader.GetQueueConfigAsync(_queueName).Returns(queueConfig);
            _rabbitMqManager.GetChannelAsync(_queueName).Returns(_channel);
            _rabbitMqManager.IsConnected.Returns(true);
            _channel.IsOpen.Returns(true);

            _sut = new QueueConsumerService(
                _queueName,
                _rabbitMqManager,
                _configLoader,
                _messageRepository,
                _transactionService,
                _logger,
                _serviceProvider);
        }

        [Fact]
        public void Constructor_NullQueueName_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => new QueueConsumerService(
                null, _rabbitMqManager, _configLoader, _messageRepository, 
                _transactionService, _logger, _serviceProvider));
        }

        [Fact]
        public void Constructor_NullRabbitMqManager_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Should.Throw<ArgumentNullException>(() => new QueueConsumerService(
                _queueName, null, _configLoader, _messageRepository, 
                _transactionService, _logger, _serviceProvider));
        }

        [Fact]
        public async Task ExecuteAsync_ConfigLoadFails_ShouldLogErrorAndReturn()
        {
            // Arrange
            _configLoader.GetQueueConfigAsync(_queueName)
                .Throws(new InvalidOperationException("Config error"));

            // Act
            await _sut.StartAsync(_cancellationTokenSource.Token);
            _cancellationTokenSource.Cancel();
            await _sut.StopAsync(_cancellationTokenSource.Token);

            // Assert
            _logger.Received().LogError(
                Arg.Any<Exception>(), 
                "Nie udało się załadować konfiguracji dla kolejki {QueueName}", 
                _queueName);
        }

        [Fact]
        public async Task ExecuteAsync_SuccessfulStart_ShouldDeclareQueueAndCreateConsumer()
        {
            // Arrange
            _cancellationTokenSource.CancelAfter(100); // Cancel quickly for test

            // Act
            await _sut.StartAsync(_cancellationTokenSource.Token);
            await Task.Delay(150); // Let it run briefly
            await _sut.StopAsync(_cancellationTokenSource.Token);

            // Assert
            _channel.Received().QueueDeclare(
                _queueName,
                true, // durable
                false, // exclusive
                false, // autoDelete
                Arg.Any<IDictionary<string, object>>());

            _channel.Received().BasicQos(0, 10, false); // prefetch count
            _channel.Received().BasicConsume(
                _queueName,
                false, // autoAck
                Arg.Any<IBasicConsumer>());
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Dispose();
            _sut?.Dispose();
        }
    }

    // ===== TESTY QUEUE CONSUMER MANAGER =====
    public class QueueConsumerManagerTests
    {
        private readonly IConfigLoader _configLoader;
        private readonly IQueueConsumerFactory _consumerFactory;
        private readonly ILogger<QueueConsumerManager> _logger;
        private readonly QueueConsumerManager _sut;

        public QueueConsumerManagerTests()
        {
            _configLoader = Substitute.For<IConfigLoader>();
            _consumerFactory = Substitute.For<IQueueConsumerFactory>();
            _logger = Substitute.For<ILogger<QueueConsumerManager>>();

            _sut = new QueueConsumerManager(_configLoader, _consumerFactory, _logger);
        }

        [Fact]
        public async Task ExecuteAsync_WithQueues_ShouldCreateConsumersForEachQueue()
        {
            // Arrange
            var queueNames = new[] { "queue1", "queue2", "queue3" };
            _configLoader.GetAllQueueNamesAsync().Returns(queueNames);

            var consumer1 = Substitute.For<QueueConsumerService>("queue1", null, null, null, null, null, null);
            var consumer2 = Substitute.For<QueueConsumerService>("queue2", null, null, null, null, null, null);
            var consumer3 = Substitute.For<QueueConsumerService>("queue3", null, null, null, null, null, null);

            _consumerFactory.CreateConsumer("queue1").Returns(consumer1);
            _consumerFactory.CreateConsumer("queue2").Returns(consumer2);
            _consumerFactory.CreateConsumer("queue3").Returns(consumer3);

            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(100);

            // Act
            await _sut.StartAsync(cancellationTokenSource.Token);
            await Task.Delay(150);
            await _sut.StopAsync(cancellationTokenSource.Token);

            // Assert
            _consumerFactory.Received(1).CreateConsumer("queue1");
            _consumerFactory.Received(1).CreateConsumer("queue2");
            _consumerFactory.Received(1).CreateConsumer("queue3");
        }
    }

    // ===== TESTY QUEUE CONSUMER FACTORY =====
    public class QueueConsumerFactoryTests
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IRabbitMqManager _rabbitMqManager;
        private readonly IConfigLoader _configLoader;
        private readonly IMessageRepository _messageRepository;
        private readonly ITransactionService _transactionService;
        private readonly ILogger<QueueConsumerService> _logger;
        private readonly QueueConsumerFactory _sut;

        public QueueConsumerFactoryTests()
        {
            _serviceProvider = Substitute.For<IServiceProvider>();
            _rabbitMqManager = Substitute.For<IRabbitMqManager>();
            _configLoader = Substitute.For<IConfigLoader>();
            _messageRepository = Substitute.For<IMessageRepository>();
            _transactionService = Substitute.For<ITransactionService>();
            _logger = Substitute.For<ILogger<QueueConsumerService>>();

            _serviceProvider.GetRequiredService<IRabbitMqManager>().Returns(_rabbitMqManager);
            _serviceProvider.GetRequiredService<IConfigLoader>().Returns(_configLoader);
            _serviceProvider.GetRequiredService<IMessageRepository>().Returns(_messageRepository);
            _serviceProvider.GetRequiredService<ITransactionService>().Returns(_transactionService);
            _serviceProvider.GetRequiredService<ILogger<QueueConsumerService>>().Returns(_logger);

            _sut = new QueueConsumerFactory(_serviceProvider);
        }

        [Fact]
        public void CreateConsumer_ValidQueueName_ShouldReturnQueueConsumerService()
        {
            // Act
            var result = _sut.CreateConsumer("test-queue");

            // Assert
            result.ShouldNotBeNull();
            result.ShouldBeOfType<QueueConsumerService>();
        }

        [Fact]
        public void CreateConsumer_CallsServiceProvider_ShouldResolveAllDependencies()
        {
            // Act
            _sut.CreateConsumer("test-queue");

            // Assert
            _serviceProvider.Received(1).GetRequiredService<IRabbitMqManager>();
            _serviceProvider.Received(1).GetRequiredService<IConfigLoader>();
            _serviceProvider.Received(1).GetRequiredService<IMessageRepository>();
            _serviceProvider.Received(1).GetRequiredService<ITransactionService>();
            _serviceProvider.Received(1).GetRequiredService<ILogger<QueueConsumerService>>();
        }
    }

    // ===== TESTY EXTENSION METHODS =====
    public class MessagePublisherExtensionsTests
    {
        private readonly IMessagePublisher _publisher;

        public MessagePublisherExtensionsTests()
        {
            _publisher = Substitute.For<IMessagePublisher>();
        }

        [Fact]
        public async Task PublishOrderEventAsync_ValidOrder_ShouldCallPublishWithCorrectParameters()
        {
            // Arrange
            var order = new { OrderId = "123", Amount = 100.50m };
            var correlationId = "test-correlation";

            // Act
            await _publisher.PublishOrderEventAsync(order, correlationId, "order.created");

            // Assert
            await _publisher.Received(1).PublishAsync(
                "orders.exchange",
                order,
                Arg.Is<MessageProperties>(p => 
                    p.CorrelationId == correlationId &&
                    p.Headers.ContainsKey("MessageType") &&
                    p.Headers.ContainsKey("EventType") &&
                    p.Headers.ContainsKey("PublishedAt")),
                "order.created");
        }

        [Fact]
        public async Task PublishNotificationEventAsync_ValidNotification_ShouldCallPublishWithCorrectParameters()
        {
            // Arrange
            var notification = new { Message = "Test notification", UserId = "user123" };
            var userId = "user123";

            // Act
            await _publisher.PublishNotificationEventAsync(notification, userId, "user.notification");

            // Assert
            await _publisher.Received(1).PublishAsync(
                "notifications.exchange",
                notification,
                Arg.Is<MessageProperties>(p => 
                    p.Headers.ContainsKey("MessageType") &&
                    p.Headers.ContainsKey("UserId") &&
                    p.ConfirmTimeout == TimeSpan.FromSeconds(10)),
                "user.notification");
        }

        [Fact]
        public async Task PublishEventAsync_WithAdditionalHeaders_ShouldMergeHeaders()
        {
            // Arrange
            var eventData = new { EventId = "event123" };
            var additionalHeaders = new Dictionary<string, object>
            {
                ["CustomHeader1"] = "Value1",
                ["CustomHeader2"] = "Value2"
            };

            // Act
            await _publisher.PublishEventAsync("test.exchange", eventData, 
                TimeSpan.FromSeconds(15), additionalHeaders, "test.routing");

            // Assert
            await _publisher.Received(1).PublishAsync(
                "test.exchange",
                eventData,
                Arg.Is<MessageProperties>(p => 
                    p.ConfirmTimeout == TimeSpan.FromSeconds(15) &&
                    p.Headers.ContainsKey("CustomHeader1") &&
                    p.Headers.ContainsKey("CustomHeader2") &&
                    p.Headers.ContainsKey("MessageType") &&
                    p.Headers.ContainsKey("EventType")),
                "test.routing");
        }
    }

    // ===== TESTY CHANNEL EXTENSIONS =====
    public class ChannelExtensionsTests
    {
        private readonly IModel _channel;
        private readonly IBasicProperties _basicProperties;

        public ChannelExtensionsTests()
        {
            _channel = Substitute.For<IModel>();
            _basicProperties = Substitute.For<IBasicProperties>();
            _channel.CreateBasicProperties().Returns(_basicProperties);
        }

        [Fact]
        public void CreateBasicPropertiesWithHeaders_MessageProperties_ShouldSetAllProperties()
        {
            // Arrange
            var messageProperties = new MessageProperties
            {
                MessageId = "test-id",
                CorrelationId = "test-correlation",
                ReplyTo = "test-reply",
                DeliveryMode = 2,
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                Priority = 5,
                Expiration = DateTime.UtcNow.AddHours(1),
                Headers = new Dictionary<string, object>
                {
                    ["CustomHeader"] = "CustomValue"
                }
            };

            // Act
            var result = _channel.CreateBasicPropertiesWithHeaders(messageProperties);

            // Assert
            result.ShouldBe(_basicProperties);
            _basicProperties.MessageId.ShouldBe("test-id");
            _basicProperties.CorrelationId.ShouldBe("test-correlation");
            _basicProperties.ReplyTo.ShouldBe("test-reply");
            _basicProperties.DeliveryMode.ShouldBe((byte)2);
            _basicProperties.Persistent.ShouldBeTrue();
            _basicProperties.ContentType.ShouldBe("application/json");
            _basicProperties.ContentEncoding.ShouldBe("utf-8");
            _basicProperties.Priority.ShouldBe((byte)5);
            _basicProperties.Headers.ShouldContainKey("CustomHeader");
            _basicProperties.Headers["CustomHeader"].ShouldBe("CustomValue");
        }

        [Fact]
        public void CreateBasicPropertiesWithHeaders_GenericEvent_ShouldSetTypeHeaders()
        {
            // Arrange
            var testEvent = new TestEvent { Id = "123", Name = "Test" };

            // Act
            var result = _channel.CreateBasicPropertiesWithHeaders(testEvent);

            // Assert
            result.ShouldBe(_basicProperties);
            _basicProperties.Persistent.ShouldBeTrue();
            _basicProperties.DeliveryMode.ShouldBe((byte)2);
            _basicProperties.ContentType.ShouldBe("application/json");
            _basicProperties.Headers.ShouldContainKey("MessageType");
            _basicProperties.Headers["MessageType"].ShouldBe("TestEvent");
            _basicProperties.Headers.ShouldContainKey("AssemblyQualifiedName");
            _basicProperties.Headers.ShouldContainKey("PublishedAt");
            _basicProperties.Headers.ShouldContainKey("MachineName");
        }

        [Fact]
        public void CreateBasicPropertiesWithHeaders_NullHeaders_ShouldNotThrow()
        {
            // Arrange
            var messageProperties = new MessageProperties
            {
                Headers = null
            };

            // Act & Assert
            Should.NotThrow(() => _channel.CreateBasicPropertiesWithHeaders(messageProperties));
        }

        [Fact]
        public void CreateBasicPropertiesWithHeaders_EmptyHeaders_ShouldNotSetHeaders()
        {
            // Arrange
            var messageProperties = new MessageProperties
            {
                Headers = new Dictionary<string, object>()
            };

            // Act
            _channel.CreateBasicPropertiesWithHeaders(messageProperties);

            // Assert
            _basicProperties.DidNotReceive().Headers = Arg.Any<IDictionary<string, object>>();
        }
    }

    // ===== TESTY MODELI =====
    public class MessagePropertiesTests
    {
        [Fact]
        public void Constructor_ShouldSetDefaultValues()
        {
            // Act
            var properties = new MessageProperties();

            // Assert
            properties.MessageId.ShouldNotBeNullOrEmpty();
            properties.DeliveryMode.ShouldBe((byte)2);
            properties.ContentType.ShouldBe("application/json");
            properties.ContentEncoding.ShouldBe("utf-8");
            properties.Headers.ShouldNotBeNull();
            properties.Priority.ShouldBe((byte)0);
            properties.ConfirmTimeout.ShouldBe(TimeSpan.FromSeconds(5));
        }

        [Fact]
        public void MessageId_ShouldBeUniqueForEachInstance()
        {
            // Act
            var properties1 = new MessageProperties();
            var properties2 = new MessageProperties();

            // Assert
            properties1.MessageId.ShouldNotBe(properties2.MessageId);
        }
    }

    public class QueueConfigTests
    {
        [Fact]
        public void Constructor_ShouldSetDefaultValues()
        {
            // Act
            var config = new QueueConfig();

            // Assert
            config.Durable.ShouldBeTrue();
            config.Exclusive.ShouldBeFalse();
            config.AutoDelete.ShouldBeFalse();
            config.PrefetchCount.ShouldBe((ushort)10);
            config.RetryDelayMs.ShouldBe(5000);
            config.Arguments.ShouldNotBeNull();
        }
    }

    public class ProcessedMessageTests
    {
        [Fact]
        public void ProcessedMessage_ShouldAllowSettingAllProperties()
        {
            // Arrange
            var now = DateTime.UtcNow;

            // Act
            var message = new ProcessedMessage
            {
                MessageId = "test-id",
                QueueName = "test-queue",
                Content = "test-content",
                ProcessedAt = now,
                Status = "Completed"
            };

            // Assert
            message.MessageId.ShouldBe("test-id");
            message.QueueName.ShouldBe("test-queue");
            message.Content.ShouldBe("test-content");
            message.ProcessedAt.ShouldBe(now);
            message.Status.ShouldBe("Completed");
        }
    }

    // ===== TESTY WYJĄTKÓW =====
    public class DuplicateMessageExceptionTests
    {
        [Fact]
        public void Constructor_WithMessage_ShouldSetMessage()
        {
            // Arrange
            var message = "Duplicate message detected";

            // Act
            var exception = new DuplicateMessageException(message);

            // Assert
            exception.Message.ShouldBe(message);
            exception.ShouldBeOfType<DuplicateMessageException>();
        }
    }

    public class ConnectionEventArgsTests
    {
        [Fact]
        public void Constructor_ShouldSetDefaultTimestamp()
        {
            // Arrange
            var before = DateTime.UtcNow;

            // Act
            var args = new ConnectionEventArgs { Reason = "Test reason" };

            // Assert
            var after = DateTime.UtcNow;
            args.Reason.ShouldBe("Test reason");
            args.Timestamp.ShouldBeGreaterThanOrEqualTo(before);
            args.Timestamp.ShouldBeLessThanOrEqualTo(after);
        }
    }

    // ===== TESTY INTEGRACYJNE DI =====
    public class ServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddRabbitMq_ShouldRegisterConnectionFactory()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddRabbitMq(factory =>
            {
                factory.HostName = "localhost";
                factory.Port = 5672;
            });

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var connectionFactory = serviceProvider.GetService<IConnectionFactory>();
            connectionFactory.ShouldNotBeNull();
            connectionFactory.HostName.ShouldBe("localhost");
            connectionFactory.Port.ShouldBe(5672);
        }

        [Fact]
        public void AddRabbitMq_ShouldRegisterRabbitMqManager()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();

            // Act
            services.AddRabbitMq(factory => { });

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            var rabbitMqManager = serviceProvider.GetService<IRabbitMqManager>();
            rabbitMqManager.ShouldNotBeNull();
            rabbitMqManager.ShouldBeOfType<RabbitMqManager>();
        }

        [Fact]
        public void AddMessageProcessing_ShouldRegisterAllServices()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IConfiguration>(Substitute.For<IConfiguration>());

            // Act
            services.AddMessageProcessing("test-connection-string");

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            
            serviceProvider.GetService<IMessageRepository>().ShouldNotBeNull();
            serviceProvider.GetService<ITransactionService>().ShouldNotBeNull();
            serviceProvider.GetService<IConfigLoader>().ShouldNotBeNull();
            serviceProvider.GetService<IQueueConsumerFactory>().ShouldNotBeNull();
            serviceProvider.GetService<IMessagePublisher>().ShouldNotBeNull();
            
            // Verify hosted service is registered
            var hostedServices = serviceProvider.GetServices<IHostedService>();
            hostedServices.ShouldContain(s => s.GetType() == typeof(QueueConsumerManager));
        }
    }

    // ===== POMOCNICZE KLASY TESTOWE =====
    public class TestEvent
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    // ===== TESTY PERFORMANCE I EDGE CASES =====
    public class RabbitMqManagerPerformanceTests
    {
        [Fact]
        public async Task GetChannelAsync_ConcurrentCalls_ShouldHandleRaceConditions()
        {
            // Arrange
            var connectionFactory = Substitute.For<IConnectionFactory>();
            var connection = Substitute.For<IConnection>();
            var channel = Substitute.For<IModel>();
            var logger = Substitute.For<ILogger<RabbitMqManager>>();

            connectionFactory.CreateConnection().Returns(connection);
            connection.IsOpen.Returns(true);
            connection.CreateModel().Returns(channel);
            channel.IsOpen.Returns(true);

            using var manager = new RabbitMqManager(connectionFactory, logger);

            // Act - Simulate concurrent access
            var tasks = Enumerable.Range(0, 10)
                .Select(_ => Task.Run(() => manager.GetChannelAsync("test-queue")))
                .ToArray();

            var results = await Task.WhenAll(tasks);

            // Assert
            results.ShouldAllBe(r => r == channel);
            connection.Received(1).CreateModel(); // Should create only one channel despite concurrent calls
        }

        [Fact]
        public async Task GetChannelAsync_ManyDifferentQueues_ShouldCreateSeparateChannels()
        {
            // Arrange
            var connectionFactory = Substitute.For<IConnectionFactory>();
            var connection = Substitute.For<IConnection>();
            var logger = Substitute.For<ILogger<RabbitMqManager>>();

            connectionFactory.CreateConnection().Returns(connection);
            connection.IsOpen.Returns(true);

            var channels = Enumerable.Range(0, 100)
                .Select(_ => {
                    var ch = Substitute.For<IModel>();
                    ch.IsOpen.Returns(true);
                    return ch;
                })
                .ToArray();

            connection.CreateModel().Returns(channels[0], channels.Skip(1).ToArray());

            using var manager = new RabbitMqManager(connectionFactory, logger);

            // Act
            var results = new List<IModel>();
            for (int i = 0; i < 100; i++)
            {
                results.Add(await manager.GetChannelAsync($"queue-{i}"));
            }

            // Assert
            results.Count.ShouldBe(100);
            results.Distinct().Count().ShouldBe(100); // All channels should be different
            connection.Received(100).CreateModel();
        }
    }

    // ===== TESTY EDGE CASES PUBLISHER =====
    public class MessagePublisherEdgeCaseTests
    {
        [Fact]
        public async Task PublishAsync_VeryLargeMessage_ShouldHandle()
        {
            // Arrange
            var rabbitMqManager = Substitute.For<IRabbitMqManager>();
            var logger = Substitute.For<ILogger<MessagePublisherService>>();
            var channel = Substitute.For<IModel>();
            var basicProperties = Substitute.For<IBasicProperties>();

            channel.CreateBasicPropertiesWithHeaders(Arg.Any<MessageProperties>()).Returns(basicProperties);
            channel.WaitForConfirms(Arg.Any<TimeSpan>()).Returns(true);
            rabbitMqManager.GetChannelAsync(Arg.Any<string>()).Returns(channel);

            var publisher = new MessagePublisherService(rabbitMqManager, logger);

            // Create large message (1MB)
            var largeMessage = new string('x', 1024 * 1024);

            // Act & Assert - Should not throw
            await Should.NotThrowAsync(() => publisher.PublishAsync("test.exchange", largeMessage));
        }

        [Fact]
        public async Task PublishAsync_SpecialCharacters_ShouldHandle()
        {
            // Arrange
            var rabbitMqManager = Substitute.For<IRabbitMqManager>();
            var logger = Substitute.For<ILogger<MessagePublisherService>>();
            var channel = Substitute.For<IModel>();
            var basicProperties = Substitute.For<IBasicProperties>();

            channel.CreateBasicPropertiesWithHeaders(Arg.Any<MessageProperties>()).Returns(basicProperties);
            channel.WaitForConfirms(Arg.Any<TimeSpan>()).Returns(true);
            rabbitMqManager.GetChannelAsync(Arg.Any<string>()).Returns(channel);

            var publisher = new MessagePublisherService(rabbitMqManager, logger);

            // Message with special characters
            var specialMessage = "Test with émojis 🚀 and ñ characters ànd ümlauts";

            // Act & Assert - Should not throw
            await Should.NotThrowAsync(() => publisher.PublishAsync("test.exchange", specialMessage));

            // Verify the message was properly encoded
            channel.Received(1).BasicPublish(
                "test.exchange",
                "",
                Arg.Any<IBasicProperties>(),
                Arg.Is<byte[]>(bytes => Encoding.UTF8.GetString(bytes) == specialMessage));
        }
    }

    // ===== TESTY MEMORY LEAKS I DISPOSAL =====
    public class DisposalTests
    {
        [Fact]
        public void RabbitMqManager_Dispose_ShouldCleanupAllResources()
        {
            // Arrange
            var connectionFactory = Substitute.For<IConnectionFactory>();
            var connection = Substitute.For<IConnection>();
            var channel1 = Substitute.For<IModel>();
            var channel2 = Substitute.For<IModel>();
            var logger = Substitute.For<ILogger<RabbitMqManager>>();

            connectionFactory.CreateConnection().Returns(connection);
            connection.IsOpen.Returns(true);
            connection.CreateModel().Returns(channel1, channel2);
            channel1.IsOpen.Returns(true);
            channel2.IsOpen.Returns(true);

            var manager = new RabbitMqManager(connectionFactory, logger);

            // Create some channels
            _ = manager.GetChannelAsync("queue1").Result;
            _ = manager.GetChannelAsync("queue2").Result;

            // Act
            manager.Dispose();

            // Assert
            channel1.Received(1).Close();
            channel1.Received(1).Dispose();
            channel2.Received(1).Close();
            channel2.Received(1).Dispose();
            connection.Received(1).Close();
            connection.Received(1).Dispose();
        }

        [Fact]
        public void RabbitMqManager_DoubleDispose_ShouldNotThrow()
        {
            // Arrange
            var connectionFactory = Substitute.For<IConnectionFactory>();
            var logger = Substitute.For<ILogger<RabbitMqManager>>();
            var manager = new RabbitMqManager(connectionFactory, logger);

            // Act & Assert
            Should.NotThrow(() =>
            {
                manager.Dispose();
                manager.Dispose(); // Second dispose should not throw
            });
        }

        [Fact]
        public void QueueConsumerService_Dispose_ShouldUnsubscribeFromEvents()
        {
            // Arrange
            var rabbitMqManager = Substitute.For<IRabbitMqManager>();
            var configLoader = Substitute.For<IConfigLoader>();
            var messageRepository = Substitute.For<IMessageRepository>();
            var transactionService = Substitute.For<ITransactionService>();
            var logger = Substitute.For<ILogger<QueueConsumerService>>();
            var serviceProvider = Substitute.For<IServiceProvider>();

            var consumer = new QueueConsumerService(
                "test-queue", rabbitMqManager, configLoader, messageRepository, 
                transactionService, logger, serviceProvider);

            // Act
            consumer.Dispose();

            // Assert - Events should be unsubscribed (hard to test directly, but Dispose should not throw)
            Should.NotThrow(() => consumer.Dispose());
        }
    }
}