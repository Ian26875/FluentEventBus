namespace FluentEventBus.RabbitMq;

/// <summary>
/// Configures publisher confirm and mandatory routing behavior for RabbitMQ publishing.
/// </summary>
public sealed class RabbitMqPublishOptions
{
    /// <summary>
    /// Default timeout waiting for a broker publisher confirm.
    /// </summary>
    public static readonly TimeSpan DefaultConfirmTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Enables RabbitMQ publisher confirms so <c>PublishAsync</c> waits for broker acknowledgment.
    /// Defaults to <c>true</c>.
    /// </summary>
    public bool PublisherConfirmEnabled { get; set; } = true;

    /// <summary>
    /// Sets the AMQP <c>mandatory</c> flag so unroutable messages are returned by the broker
    /// instead of being silently dropped. Defaults to <c>true</c>.
    /// </summary>
    public bool Mandatory { get; set; } = true;

    /// <summary>
    /// Maximum duration to wait for broker publisher confirmation when
    /// <see cref="PublisherConfirmEnabled"/> is <c>true</c>. Defaults to 10 seconds.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = DefaultConfirmTimeout;

    internal void Validate()
    {
        if (ConfirmTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ConfirmTimeout),
                ConfirmTimeout,
                "ConfirmTimeout cannot be negative.");
        }

        if (PublisherConfirmEnabled && ConfirmTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ConfirmTimeout),
                ConfirmTimeout,
                "ConfirmTimeout must be greater than TimeSpan.Zero when PublisherConfirmEnabled is true.");
        }
    }
}
