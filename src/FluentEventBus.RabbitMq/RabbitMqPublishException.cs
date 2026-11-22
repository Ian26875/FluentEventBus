namespace FluentEventBus.RabbitMq;

/// <summary>
/// Classifies failures reported when publishing an event to RabbitMQ.
/// </summary>
public enum RabbitMqPublishFailureReason
{
    /// <summary>
    /// The broker explicitly rejected (nacked) the published message.
    /// </summary>
    BrokerRejected,

    /// <summary>
    /// The mandatory message could not be routed to any bound queue and was returned by the broker.
    /// </summary>
    Unroutable,

    /// <summary>
    /// The broker did not confirm the message within the configured confirm timeout.
    /// </summary>
    ConfirmTimeout,

    /// <summary>
    /// The AMQP channel closed or shut down before publish confirmation completed.
    /// </summary>
    ChannelShutdown,

    /// <summary>
    /// The RabbitMQ connection was interrupted or unreachable during publish.
    /// </summary>
    ConnectionInterrupted
}

/// <summary>
/// Represents an error that occurred while publishing an event to RabbitMQ.
/// </summary>
public class RabbitMqPublishException : Exception
{
    public RabbitMqPublishException(
        RabbitMqPublishFailureReason reason,
        string exchange,
        string routingKey,
        string message,
        Exception? innerException = null)
        : base(message, innerException)
    {
        Reason = reason;
        Exchange = exchange;
        RoutingKey = routingKey;
    }

    public RabbitMqPublishFailureReason Reason { get; }

    public string Exchange { get; }

    public string RoutingKey { get; }
}

/// <summary>
/// Exception thrown when RabbitMQ returns a mandatory message because no queue binding matched.
/// </summary>
public sealed class RabbitMqMessageReturnedException : RabbitMqPublishException
{
    public RabbitMqMessageReturnedException(
        string exchange,
        string routingKey,
        string? returnReason = null,
        Exception? innerException = null)
        : base(
            RabbitMqPublishFailureReason.Unroutable,
            exchange,
            routingKey,
            BuildMessage(exchange, routingKey, returnReason),
            innerException)
    {
        ReturnReason = returnReason;
    }

    public string? ReturnReason { get; }

    private static string BuildMessage(string exchange, string routingKey, string? returnReason)
    {
        return string.IsNullOrWhiteSpace(returnReason)
            ? $"Message published to exchange '{exchange}' with routing key '{routingKey}' was returned as unroutable by RabbitMQ."
            : $"Message published to exchange '{exchange}' with routing key '{routingKey}' was returned as unroutable by RabbitMQ (reason: {returnReason}).";
    }
}

/// <summary>
/// Exception thrown when RabbitMQ does not confirm a published message before <see cref="RabbitMqPublishOptions.ConfirmTimeout"/> elapses.
/// </summary>
public sealed class RabbitMqPublishTimeoutException : RabbitMqPublishException
{
    public RabbitMqPublishTimeoutException(
        string exchange,
        string routingKey,
        TimeSpan timeout,
        string? message = null,
        Exception? innerException = null)
        : base(
            RabbitMqPublishFailureReason.ConfirmTimeout,
            exchange,
            routingKey,
            message ?? $"Timed out after {timeout} waiting for RabbitMQ publisher confirm on exchange '{exchange}' with routing key '{routingKey}'.",
            innerException)
    {
        Timeout = timeout;
    }

    public TimeSpan Timeout { get; }
}
