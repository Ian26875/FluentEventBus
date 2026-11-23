using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.DI;
using EasyNetQ.Topology;

namespace FluentEventBus.RabbitMqTests.Fakes;

internal sealed class FakeAdvancedBus : IAdvancedBus
{
    private EventHandler<MessageReturnedEventArgs>? _messageReturned;
    private EventHandler<DisconnectedEventArgs>? _disconnected;

    public Func<Exchange, string, bool, MessageProperties, ReadOnlyMemory<byte>, CancellationToken, Task>? OnPublish { get; init; }

    public List<Exchange> DeclaredExchanges { get; } = new();
    public List<Queue> DeclaredQueues { get; } = new();
    public List<RecordedBinding> QueueBindings { get; } = new();
    public List<RecordedPublish> PublishedMessages { get; } = new();
    public List<FakeRegisteredConsumer> RegisteredConsumers { get; } = new();

    public int ExchangeDeclareCount => DeclaredExchanges.Count;
    public Exchange LastExchange { get; private set; }
    public string? LastRoutingKey { get; private set; }
    public bool LastMandatory { get; private set; }
    public MessageProperties? LastProperties { get; private set; }

    public int MessageReturnedSubscriberCount => _messageReturned?.GetInvocationList().Length ?? 0;
    public int DisconnectedSubscriberCount => _disconnected?.GetInvocationList().Length ?? 0;

    public void RaiseMessageReturned(MessageReturnedEventArgs args) => _messageReturned?.Invoke(this, args);
    public void RaiseDisconnected(DisconnectedEventArgs args) => _disconnected?.Invoke(this, args);

    public bool IsConnected => false;
    public IServiceResolver Container => throw new NotSupportedException();
    public IConventions Conventions => throw new NotSupportedException();

    public event EventHandler<ConnectedEventArgs>? Connected { add { } remove { } }
    public event EventHandler<DisconnectedEventArgs>? Disconnected
    {
        add => _disconnected += value;
        remove => _disconnected -= value;
    }
    public event EventHandler<BlockedEventArgs>? Blocked { add { } remove { } }
    public event EventHandler<UnblockedEventArgs>? Unblocked { add { } remove { } }
    public event EventHandler<MessageReturnedEventArgs>? MessageReturned
    {
        add => _messageReturned += value;
        remove => _messageReturned -= value;
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public IDisposable Consume(Action<IConsumeConfiguration> configure)
    {
        var config = new FakeConsumeConfiguration();
        configure(config);

        var handle = new FakeConsumerHandle();
        foreach (var entry in config.Queues)
        {
            RegisteredConsumers.Add(new FakeRegisteredConsumer(
                entry.Queue,
                entry.Handler,
                config.PrefetchCount,
                handle));
        }

        return handle;
    }

    public Task<Exchange> ExchangeDeclareAsync(
        string name,
        Action<IExchangeDeclareConfiguration> configure,
        CancellationToken cancellationToken = default)
    {
        var cfg = new FakeExchangeDeclareConfiguration();
        configure(cfg);
        var exchange = new Exchange(
            name,
            cfg.Type ?? ExchangeType.Topic,
            cfg.IsDurable,
            cfg.IsAutoDelete,
            cfg.Arguments);
        DeclaredExchanges.Add(exchange);
        return Task.FromResult(exchange);
    }

    public Task<Queue> QueueDeclareAsync(
        string name,
        Action<IQueueDeclareConfiguration> configure,
        CancellationToken cancellationToken = default)
    {
        var cfg = new FakeQueueDeclareConfiguration();
        configure(cfg);
        var queue = new Queue(name, cfg.IsDurable, cfg.IsExclusive, cfg.IsAutoDelete, cfg.Arguments);
        DeclaredQueues.Add(queue);
        return Task.FromResult(queue);
    }

    public Task<Binding<Queue>> BindAsync(
        Exchange exchange,
        Queue queue,
        string routingKey,
        IDictionary<string, object>? arguments,
        CancellationToken cancellationToken = default)
    {
        QueueBindings.Add(new RecordedBinding(exchange.Name, queue.Name, routingKey, arguments));
        return Task.FromResult(new Binding<Queue>(exchange, queue, routingKey, arguments!));
    }

    public Task PublishAsync(
        Exchange exchange,
        string routingKey,
        bool mandatory,
        MessageProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default)
    {
        LastExchange = exchange;
        LastRoutingKey = routingKey;
        LastMandatory = mandatory;
        LastProperties = properties;
        PublishedMessages.Add(new RecordedPublish(exchange, routingKey, mandatory, properties, body));

        return OnPublish is not null
            ? OnPublish(exchange, routingKey, mandatory, properties, body, cancellationToken)
            : Task.CompletedTask;
    }

    public Task PublishAsync(Exchange exchange, string routingKey, bool mandatory, IMessage message, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task PublishAsync<T>(Exchange exchange, string routingKey, bool mandatory, IMessage<T> message, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<Queue> QueueDeclareAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task QueueDeclarePassiveAsync(string name, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task QueueDeleteAsync(string name, bool ifUnused = false, bool ifEmpty = false, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task QueuePurgeAsync(string name, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task ExchangeDeclarePassiveAsync(string name, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task ExchangeDeleteAsync(Exchange exchange, bool ifUnused = false, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<Binding<Exchange>> BindAsync(Exchange source, Exchange destination, string routingKey, IDictionary<string, object> arguments, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task QueueUnbindAsync(string queue, string exchange, string routingKey, IDictionary<string, object> arguments, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task ExchangeUnbindAsync(string destinationExchange, string sourceExchange, string routingKey, IDictionary<string, object> arguments, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<QueueStats> GetQueueStatsAsync(string name, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public IPullingConsumer<PullResult> CreatePullingConsumer(in Queue queue, bool autoAck = true)
        => throw new NotSupportedException();

    public IPullingConsumer<PullResult<T>> CreatePullingConsumer<T>(in Queue queue, bool autoAck = true)
        => throw new NotSupportedException();

    public void Dispose() { }

    internal sealed record RecordedBinding(string ExchangeName, string QueueName, string RoutingKey, IDictionary<string, object>? Arguments);
    internal sealed record RecordedPublish(Exchange Exchange, string RoutingKey, bool Mandatory, MessageProperties Properties, ReadOnlyMemory<byte> Body);
    internal sealed record FakeRegisteredConsumer(Queue Queue, MessageHandler Handler, ushort? PrefetchCount, FakeConsumerHandle Handle);

    internal sealed class FakeConsumerHandle : IDisposable
    {
        public bool IsDisposed { get; private set; }
        public void Dispose() => IsDisposed = true;
    }

    private sealed class FakeExchangeDeclareConfiguration : IExchangeDeclareConfiguration
    {
        public bool IsDurable { get; private set; } = true;
        public bool IsAutoDelete { get; private set; }
        public string? Type { get; private set; }
        public IDictionary<string, object>? Arguments { get; private set; }

        public IExchangeDeclareConfiguration AsDurable(bool isDurable)
        {
            IsDurable = isDurable;
            return this;
        }

        public IExchangeDeclareConfiguration AsAutoDelete(bool isAutoDelete)
        {
            IsAutoDelete = isAutoDelete;
            return this;
        }

        public IExchangeDeclareConfiguration WithType(string type)
        {
            Type = type;
            return this;
        }

        public IExchangeDeclareConfiguration WithArgument(string name, object value)
        {
            (Arguments ??= new Dictionary<string, object>())[name] = value;
            return this;
        }
    }

    private sealed class FakeQueueDeclareConfiguration : IQueueDeclareConfiguration
    {
        public bool IsDurable { get; private set; } = true;
        public bool IsExclusive { get; private set; }
        public bool IsAutoDelete { get; private set; }
        public IDictionary<string, object>? Arguments { get; private set; }

        public IQueueDeclareConfiguration AsDurable(bool isDurable)
        {
            IsDurable = isDurable;
            return this;
        }

        public IQueueDeclareConfiguration AsExclusive(bool isExclusive)
        {
            IsExclusive = isExclusive;
            return this;
        }

        public IQueueDeclareConfiguration AsAutoDelete(bool isAutoDelete)
        {
            IsAutoDelete = isAutoDelete;
            return this;
        }

        public IQueueDeclareConfiguration WithArgument(string name, object value)
        {
            (Arguments ??= new Dictionary<string, object>())[name] = value;
            return this;
        }
    }

    private sealed class FakeConsumeConfiguration : IConsumeConfiguration
    {
        public ushort? PrefetchCount { get; private set; }
        public List<(Queue Queue, MessageHandler Handler)> Queues { get; } = new();

        public IConsumeConfiguration WithPrefetchCount(ushort prefetchCount)
        {
            PrefetchCount = prefetchCount;
            return this;
        }

        public IConsumeConfiguration ForQueue(in Queue queue, MessageHandler handler, Action<IPerQueueConsumeConfiguration> configure)
        {
            var perQueue = new FakePerQueueConsumeConfiguration();
            configure(perQueue);
            Queues.Add((queue, handler));
            return this;
        }

        public IConsumeConfiguration ForQueue(in Queue queue, Action<IHandlerRegistration> register, Action<IPerQueueConsumeConfiguration> configure)
            => throw new NotSupportedException();
    }

    private sealed class FakePerQueueConsumeConfiguration : IPerQueueConsumeConfiguration
    {
        public IPerQueueConsumeConfiguration WithAutoAck() => this;
        public IPerQueueConsumeConfiguration WithConsumerTag(string consumerTag) => this;
        public IPerQueueConsumeConfiguration WithExclusive(bool isExclusive = true) => this;
        public IPerQueueConsumeConfiguration WithArgument(string name, object value) => this;
    }
}
