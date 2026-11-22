using EasyNetQ;
using EasyNetQ.ConnectionString;
using Microsoft.Extensions.Options;

namespace FluentEventBus.RabbitMq;

/// <summary>
/// Owns the single EasyNetQ bus shared by all RabbitMQ components
/// (publisher and subscriber), so the application holds one broker connection.
/// </summary>
public sealed class RabbitMqConnectionProvider : IDisposable
{
    private readonly Lazy<IBus> _bus;

    public RabbitMqConnectionProvider(
        IOptions<RabbitMqConnectionOption> options,
        IOptions<RabbitMqPublishOptions>? publishOptions = null)
    {
        var connectionOption = options?.Value ?? throw new ArgumentNullException(nameof(options));
        var publishOption = publishOptions?.Value ?? new RabbitMqPublishOptions();
        ValidateOptions(connectionOption);
        publishOption.Validate();

        _bus = new Lazy<IBus>(() =>
            RabbitHutch.CreateBus(BuildConnectionConfiguration(connectionOption, publishOption), _ => { }));
    }

    /// <summary>
    /// Gets the shared advanced bus. The underlying connection is created lazily on first use.
    /// </summary>
    public IAdvancedBus GetAdvancedBus()
    {
        return _bus.Value.Advanced;
    }

    internal static ConnectionConfiguration BuildConnectionConfiguration(
        RabbitMqConnectionOption connectionOption,
        RabbitMqPublishOptions publishOption)
    {
        if (connectionOption is null)
        {
            throw new ArgumentNullException(nameof(connectionOption));
        }

        if (publishOption is null)
        {
            throw new ArgumentNullException(nameof(publishOption));
        }

        ValidateOptions(connectionOption);
        publishOption.Validate();

        var connectionString = $"amqp://{Uri.EscapeDataString(connectionOption.UserName)}:{Uri.EscapeDataString(connectionOption.Password)}@{connectionOption.Host}/";
        var config = new AmqpConnectionStringParser().Parse(connectionString);
        config.PublisherConfirms = publishOption.PublisherConfirmEnabled;
        config.MandatoryPublish = publishOption.Mandatory;

        // Ensure EasyNetQ's internal operation timeout does not preempt a longer user ConfirmTimeout;
        // shorter timeouts are enforced by RabbitMqEventPublisher's linked CancellationTokenSource.
        if (publishOption.PublisherConfirmEnabled && publishOption.ConfirmTimeout > config.Timeout)
        {
            config.Timeout = publishOption.ConfirmTimeout;
        }

        return config;
    }

    private static void ValidateOptions(RabbitMqConnectionOption connectionOption)
    {
        if (string.IsNullOrWhiteSpace(connectionOption.UserName))
        {
            throw new ArgumentException("RabbitMqOption.UserName is required.", nameof(connectionOption));
        }

        if (string.IsNullOrWhiteSpace(connectionOption.Password))
        {
            throw new ArgumentException("RabbitMqOption.Password is required.", nameof(connectionOption));
        }

        if (string.IsNullOrWhiteSpace(connectionOption.Host))
        {
            throw new ArgumentException("RabbitMqOption.Host is required.", nameof(connectionOption));
        }
    }

    public void Dispose()
    {
        if (_bus.IsValueCreated)
        {
            _bus.Value.Dispose();
        }
    }
}
