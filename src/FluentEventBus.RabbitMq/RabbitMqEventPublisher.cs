using System.Collections.Concurrent;
using System.Text;
using EasyNetQ;
using EasyNetQ.Persistent;
using EasyNetQ.Producer;
using EasyNetQ.Topology;
using FluentEventBus.Event;
using FluentEventBus.Naming;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;
using ISerializer = FluentEventBus.Serialization.ISerializer;

namespace FluentEventBus.RabbitMq;

/// <summary>
/// Publishes events to RabbitMQ with optional publisher confirms and mandatory return handling.
/// </summary>
/// <seealso cref="AbstractEventPublisher"/>
public class RabbitMqEventPublisher : AbstractEventPublisher, IDisposable
{
    private readonly RabbitMqBindingOption _rabbitMqBindingOption;
    private readonly RabbitMqPublishOptions _publishOptions;
    private readonly Func<IAdvancedBus> _advancedBusFactory;
    private readonly ILogger<RabbitMqEventPublisher> _logger;

    /// <summary>
    /// Exchanges already declared on the broker, keyed by name. Evicted when channel or
    /// connection failures occur so a reconnected broker/virtual-host is re-declared cleanly.
    /// </summary>
    private readonly ConcurrentDictionary<string, Exchange> _declaredExchanges = new();

    /// <summary>
    /// Correlates in-flight mandatory publishes (by MessageId) with asynchronous MessageReturned
    /// callbacks so the exception thrown to the caller carries the broker's return reason.
    /// </summary>
    private readonly ConcurrentDictionary<string, ReturnedMessageBox> _pendingReturns = new();

    private readonly object _busLock = new();
    private IAdvancedBus? _subscribedBus;
    private long _disconnectGeneration;
    private bool _disposed;

    public RabbitMqEventPublisher(
        ISerializer serializer,
        IEventNameRegistry eventMapper,
        RabbitMqConnectionProvider connectionProvider,
        IOptions<RabbitMqBindingOption> rabbitMqBindingOptions,
        ILogger<RabbitMqEventPublisher> logger,
        IOptions<RabbitMqPublishOptions>? publishOptions = null)
        : this(
            serializer,
            eventMapper,
            (connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider))).GetAdvancedBus,
            rabbitMqBindingOptions,
            publishOptions,
            logger)
    {
    }

    internal RabbitMqEventPublisher(
        ISerializer serializer,
        IEventNameRegistry eventMapper,
        Func<IAdvancedBus> advancedBusFactory,
        IOptions<RabbitMqBindingOption> rabbitMqBindingOptions,
        IOptions<RabbitMqPublishOptions>? publishOptions,
        ILogger<RabbitMqEventPublisher> logger)
        : base(serializer, eventMapper)
    {
        _advancedBusFactory = advancedBusFactory ?? throw new ArgumentNullException(nameof(advancedBusFactory));
        _rabbitMqBindingOption = rabbitMqBindingOptions?.Value ?? throw new ArgumentNullException(nameof(rabbitMqBindingOptions));
        _publishOptions = publishOptions?.Value ?? new RabbitMqPublishOptions();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _publishOptions.Validate();

        if (_publishOptions.Mandatory && !_publishOptions.PublisherConfirmEnabled)
        {
            _logger.LogWarning(
                "RabbitMqPublishOptions has Mandatory=true with PublisherConfirmEnabled=false. " +
                "Unroutable messages will be logged via MessageReturned, but PublishAsync cannot throw synchronously without publisher confirms enabled.");
        }
    }

    protected override async Task PublishEventAsync(EventContext eventContext, CancellationToken cancellationToken = default)
    {
        if (eventContext is null)
        {
            throw new ArgumentNullException(nameof(eventContext));
        }

        var exchangeName = ResolveExchangeName(eventContext.EventName);
        var routeKey = eventContext.EventName;
        var advancedBus = GetBus();
        var disconnectGeneration = Volatile.Read(ref _disconnectGeneration);

        using var timeoutCts = _publishOptions.PublisherConfirmEnabled
            ? new CancellationTokenSource(_publishOptions.ConfirmTimeout)
            : null;
        using var linkedCts = timeoutCts is not null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)
            : null;
        var effectiveToken = linkedCts?.Token ?? cancellationToken;

        var messageId = eventContext.Headers.MessageId;
        var returnBox = _publishOptions.Mandatory && !string.IsNullOrEmpty(messageId)
            ? new ReturnedMessageBox()
            : null;
        var trackedReturn = returnBox is not null && _pendingReturns.TryAdd(messageId!, returnBox);

        try
        {
            _logger.LogTrace("Declaring RabbitMQ exchange '{Exchange}' to publish event '{EventName}'...", exchangeName, routeKey);
            var exchange = await GetOrDeclareExchangeAsync(advancedBus, exchangeName, effectiveToken);

            _logger.LogTrace("Publishing event '{EventName}' to RabbitMQ exchange '{Exchange}'...", routeKey, exchangeName);

            // Copy headers so EasyNetQ's internal confirmation-id mutation never leaks into caller state.
            var properties = new MessageProperties
            {
                DeliveryMode = DeliveryMode.Persistent,
                Headers = new Dictionary<string, object>(eventContext.Headers)
            };

            await advancedBus.PublishAsync(
                exchange,
                routeKey,
                _publishOptions.Mandatory,
                properties,
                eventContext.Data,
                effectiveToken);

            if (returnBox?.Info is { } returnedInfo)
            {
                throw new RabbitMqMessageReturnedException(exchangeName, routeKey, returnedInfo.ReturnReason);
            }
        }
        catch (RabbitMqPublishException)
        {
            throw;
        }
        catch (PublishReturnedException ex)
        {
            throw new RabbitMqMessageReturnedException(exchangeName, routeKey, returnBox?.Info?.ReturnReason, ex);
        }
        catch (PublishNackedException ex)
        {
            throw new RabbitMqPublishException(
                RabbitMqPublishFailureReason.BrokerRejected,
                exchangeName,
                routeKey,
                $"RabbitMQ broker rejected (nacked) publish to exchange '{exchangeName}' with routing key '{routeKey}'.",
                ex);
        }
        catch (PublishInterruptedException ex)
        {
            _declaredExchanges.TryRemove(exchangeName, out _);
            throw new RabbitMqPublishException(
                RabbitMqPublishFailureReason.ConnectionInterrupted,
                exchangeName,
                routeKey,
                $"RabbitMQ publish to exchange '{exchangeName}' with routing key '{routeKey}' was interrupted.",
                ex);
        }
        catch (OperationInterruptedException ex)
        {
            _declaredExchanges.TryRemove(exchangeName, out _);
            var reason = IsConnectionShutdown(ex)
                ? RabbitMqPublishFailureReason.ConnectionInterrupted
                : RabbitMqPublishFailureReason.ChannelShutdown;
            var description = reason == RabbitMqPublishFailureReason.ConnectionInterrupted
                ? "RabbitMQ connection closed"
                : "RabbitMQ channel shut down";
            throw new RabbitMqPublishException(
                reason,
                exchangeName,
                routeKey,
                $"{description} while publishing to exchange '{exchangeName}' with routing key '{routeKey}': {ex.Message}",
                ex);
        }
        catch (BrokerUnreachableException ex)
        {
            _declaredExchanges.TryRemove(exchangeName, out _);
            throw new RabbitMqPublishException(
                RabbitMqPublishFailureReason.ConnectionInterrupted,
                exchangeName,
                routeKey,
                $"RabbitMQ broker is unreachable while publishing to exchange '{exchangeName}' with routing key '{routeKey}'.",
                ex);
        }
        catch (ConnectFailureException ex)
        {
            _declaredExchanges.TryRemove(exchangeName, out _);
            throw new RabbitMqPublishException(
                RabbitMqPublishFailureReason.ConnectionInterrupted,
                exchangeName,
                routeKey,
                $"Failed to connect to RabbitMQ while publishing to exchange '{exchangeName}' with routing key '{routeKey}'.",
                ex);
        }
        catch (OperationCanceledException ex) when (timeoutCts is not null && timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            if (Volatile.Read(ref _disconnectGeneration) != disconnectGeneration)
            {
                _declaredExchanges.TryRemove(exchangeName, out _);
                throw new RabbitMqPublishException(
                    RabbitMqPublishFailureReason.ConnectionInterrupted,
                    exchangeName,
                    routeKey,
                    $"RabbitMQ producer connection was disconnected while waiting for publish confirmation on exchange '{exchangeName}' with routing key '{routeKey}'.",
                    ex);
            }

            throw new RabbitMqPublishTimeoutException(exchangeName, routeKey, _publishOptions.ConfirmTimeout, innerException: ex);
        }
        catch (TimeoutException ex)
        {
            throw new RabbitMqPublishTimeoutException(exchangeName, routeKey, _publishOptions.ConfirmTimeout, ex.Message, ex);
        }
        catch (EasyNetQException ex)
        {
            _declaredExchanges.TryRemove(exchangeName, out _);
            throw new RabbitMqPublishException(
                RabbitMqPublishFailureReason.ConnectionInterrupted,
                exchangeName,
                routeKey,
                $"RabbitMQ transport error while publishing to exchange '{exchangeName}' with routing key '{routeKey}': {ex.Message}",
                ex);
        }
        finally
        {
            if (trackedReturn)
            {
                _pendingReturns.TryRemove(new KeyValuePair<string, ReturnedMessageBox>(messageId!, returnBox!));
            }
        }
    }

    public void Dispose()
    {
        IAdvancedBus? bus;
        lock (_busLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            bus = _subscribedBus;
            _subscribedBus = null;
        }

        if (bus is not null)
        {
            bus.MessageReturned -= OnMessageReturned;
            bus.Disconnected -= OnDisconnected;
        }
    }

    private string ResolveExchangeName(string eventName)
    {
        var exchangeName = _rabbitMqBindingOption.ExchangeBindings.TryGetValue(eventName, out var bindingExchangeName)
            ? bindingExchangeName
            : _rabbitMqBindingOption.GlobalExchange;

        if (string.IsNullOrWhiteSpace(exchangeName))
        {
            throw new InvalidOperationException($"Exchange is not configured for event '{eventName}'.");
        }

        return exchangeName;
    }

    private async Task<Exchange> GetOrDeclareExchangeAsync(IAdvancedBus advancedBus, string exchangeName, CancellationToken cancellationToken)
    {
        if (_declaredExchanges.TryGetValue(exchangeName, out var cachedExchange))
        {
            return cachedExchange;
        }

        // A concurrent duplicate declaration is harmless: exchange declaration is idempotent.
        var exchange = await advancedBus.ExchangeDeclareAsync(
            exchangeName,
            configure: configuration =>
            {
                configuration.WithType(ExchangeType.Topic);
            },
            cancellationToken);
        _declaredExchanges.TryAdd(exchangeName, exchange);
        return exchange;
    }

    private IAdvancedBus GetBus()
    {
        lock (_busLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMqEventPublisher));
            }

            if (_subscribedBus is not null)
            {
                return _subscribedBus;
            }

            var bus = _advancedBusFactory();
            bus.MessageReturned += OnMessageReturned;
            bus.Disconnected += OnDisconnected;
            _subscribedBus = bus;
            return bus;
        }
    }

    private void OnMessageReturned(object? sender, MessageReturnedEventArgs args)
    {
        var messageId = TryGetMessageId(args.MessageProperties);
        if (!string.IsNullOrEmpty(messageId) && _pendingReturns.TryGetValue(messageId, out var box))
        {
            box.Info = args.MessageReturnedInfo;
        }

        _logger.LogError(
            "RabbitMQ returned unroutable message for exchange '{Exchange}', routing key '{RoutingKey}', messageId '{MessageId}': {ReturnReason}",
            args.MessageReturnedInfo.Exchange,
            args.MessageReturnedInfo.RoutingKey,
            messageId ?? "(none)",
            args.MessageReturnedInfo.ReturnReason);
    }

    private void OnDisconnected(object? sender, DisconnectedEventArgs args)
    {
        if (args.Type != PersistentConnectionType.Producer)
        {
            return;
        }

        Interlocked.Increment(ref _disconnectGeneration);
        _declaredExchanges.Clear();
        _logger.LogWarning(
            "RabbitMQ producer connection disconnected from {Host}:{Port}: {Reason}",
            args.Hostname,
            args.Port,
            args.Reason);
    }

    private static bool IsConnectionShutdown(OperationInterruptedException ex)
    {
        return ex.ShutdownReason is { ClassId: 10 } or { ReplyCode: 320 };
    }

    private static string? TryGetMessageId(MessageProperties? properties)
    {
        if (properties?.Headers is null || !properties.Headers.TryGetValue(Headers.MessageIdKey, out var value))
        {
            return null;
        }

        return value switch
        {
            string text => text,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            _ => value?.ToString()
        };
    }

    private sealed class ReturnedMessageBox
    {
        private volatile ReturnedPayload? _payload;

        public MessageReturnedInfo? Info
        {
            get => _payload?.Info;
            set => _payload = value is { } info ? new ReturnedPayload(info) : null;
        }

        private sealed class ReturnedPayload
        {
            public ReturnedPayload(MessageReturnedInfo info) => Info = info;
            public MessageReturnedInfo Info { get; }
        }
    }
}
