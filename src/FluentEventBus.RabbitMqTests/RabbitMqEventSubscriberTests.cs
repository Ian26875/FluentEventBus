using System.Text;
using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.Topology;
using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.RabbitMq;
using FluentEventBus.RabbitMqTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqEventSubscriberTests
{
    [Fact(DisplayName = "SubscribeAsync_WithDefaults_ShouldDeclareDefaultExchangeAndServiceEnvQueueAndBind")]
    public async Task SubscribeAsync_WithDefaults_ShouldDeclareDefaultExchangeAndServiceEnvQueueAndBind()
    {
        var fakeBus = new FakeAdvancedBus();
        var options = new RabbitMqBindingOption
        {
            ServiceName = "orders",
            EnvironmentName = "staging"
        };
        using var subscriber = CreateSubscriber(fakeBus, options);

        await subscriber.SubscribeAsync(new List<string> { "orders.order.placed.v1" });

        fakeBus.DeclaredExchanges.Should().ContainSingle(e => e.Name == "eventbus.topic" && e.Type == ExchangeType.Topic);
        fakeBus.DeclaredQueues.Should().ContainSingle(q => q.Name == "orders.staging");
        fakeBus.QueueBindings.Should().ContainSingle(b =>
            b.ExchangeName == "eventbus.topic" &&
            b.QueueName == "orders.staging" &&
            b.RoutingKey == "orders.order.placed.v1");
        fakeBus.RegisteredConsumers.Should().ContainSingle(c => c.Queue.Name == "orders.staging");
    }

    [Fact(DisplayName = "SubscribeAsync_WithPerEventBinding_ShouldOverrideGlobalExchangeAndQueue")]
    public async Task SubscribeAsync_WithPerEventBinding_ShouldOverrideGlobalExchangeAndQueue()
    {
        var fakeBus = new FakeAdvancedBus();
        var options = new RabbitMqBindingOption
        {
            GlobalExchange = "global.topic",
            GlobalQueue = "global.queue",
            ExchangeBindings = { ["order.created.v1"] = "orders.exchange" },
            QueueBindings = { ["order.created.v1"] = "orders.queue" }
        };
        using var subscriber = CreateSubscriber(fakeBus, options);

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1", "user.updated.v1" });

        fakeBus.QueueBindings.Should().Contain(b =>
            b.ExchangeName == "orders.exchange" &&
            b.QueueName == "orders.queue" &&
            b.RoutingKey == "order.created.v1");
        fakeBus.QueueBindings.Should().Contain(b =>
            b.ExchangeName == "global.topic" &&
            b.QueueName == "global.queue" &&
            b.RoutingKey == "user.updated.v1");
    }

    [Fact(DisplayName = "SubscribeAsync_WithDeadLetter_ShouldDeclareDlxDlqBindWildcardAndPassDeadLetterArgument")]
    public async Task SubscribeAsync_WithDeadLetter_ShouldDeclareDlxDlqBindWildcardAndPassDeadLetterArgument()
    {
        var fakeBus = new FakeAdvancedBus();
        var options = new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue",
            GlobalDeadLetterExchange = "orders.dlx",
            GlobalDeadLetterQueue = "orders.dlq"
        };
        using var subscriber = CreateSubscriber(fakeBus, options);

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        fakeBus.DeclaredExchanges.Should().Contain(e => e.Name == "orders.dlx" && e.Type == ExchangeType.Topic);
        fakeBus.DeclaredQueues.Should().Contain(q => q.Name == "orders.dlq");
        fakeBus.QueueBindings.Should().Contain(b =>
            b.ExchangeName == "orders.dlx" &&
            b.QueueName == "orders.dlq" &&
            b.RoutingKey == "#");

        var mainQueue = fakeBus.DeclaredQueues.Single(q => q.Name == "orders.queue");
        mainQueue.Arguments.Should().ContainKey("x-dead-letter-exchange")
            .WhoseValue.Should().Be("orders.dlx");
    }

    [Fact(DisplayName = "SubscribeAsync_MessageReceived_ShouldDecodeByteArrayHeadersInvokeConsumerAndAck")]
    public async Task SubscribeAsync_MessageReceived_ShouldDecodeByteArrayHeadersInvokeConsumerAndAck()
    {
        var fakeBus = new FakeAdvancedBus();
        using var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue"
        });

        EventContext? captured = null;
        subscriber.ConsumerReceived = ctx =>
        {
            captured = ctx;
            return Task.CompletedTask;
        };

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        var consumer = fakeBus.RegisteredConsumers.Single();
        var payload = Encoding.UTF8.GetBytes("{\"id\":1}");
        var props = new MessageProperties
        {
            Headers = new Dictionary<string, object>
            {
                [Headers.MessageIdKey] = Encoding.UTF8.GetBytes("msg-99"),
                [Headers.CorrelationIdKey] = Encoding.UTF8.GetBytes("corr-77"),
                ["numeric-header"] = 42
            }
        };
        var info = new MessageReceivedInfo("ctag", 1UL, false, "orders.topic", "order.created.v1", "orders.queue");

        var ack = await consumer.Handler(payload, props, info, CancellationToken.None);

        ack.Should().Be(AckStrategies.Ack);
        captured.Should().NotBeNull();
        captured!.EventName.Should().Be("order.created.v1");
        captured.Data.ToArray().Should().Equal(payload);
        captured.Headers.MessageId.Should().Be("msg-99");
        captured.Headers.CorrelationId.Should().Be("corr-77");
        captured.Headers["numeric-header"].Should().Be(42);
    }

    [Fact(DisplayName = "SubscribeAsync_WhenConsumerReceivedNotSet_ShouldReturnNackWithRequeue")]
    public async Task SubscribeAsync_WhenConsumerReceivedNotSet_ShouldReturnNackWithRequeue()
    {
        var fakeBus = new FakeAdvancedBus();
        using var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue"
        });

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        var consumer = fakeBus.RegisteredConsumers.Single();
        var info = new MessageReceivedInfo("ctag", 1UL, false, "orders.topic", "order.created.v1", "orders.queue");

        var ack = await consumer.Handler(ReadOnlyMemory<byte>.Empty, new MessageProperties(), info, CancellationToken.None);

        ack.Should().Be(AckStrategies.NackWithRequeue);
    }

    [Fact(DisplayName = "SubscribeAsync_HandlerFailsBelowMaxRetry_ShouldRepublishWithIncrementedRetryCountAndAck")]
    public async Task SubscribeAsync_HandlerFailsBelowMaxRetry_ShouldRepublishWithIncrementedRetryCountAndAck()
    {
        var fakeBus = new FakeAdvancedBus();
        using var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue",
            MaxRetryCount = 3
        });
        subscriber.ConsumerReceived = _ => throw new InvalidOperationException("transient");

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        var consumer = fakeBus.RegisteredConsumers.Single();
        var payload = Encoding.UTF8.GetBytes("{\"id\":1}");
        var props = new MessageProperties
        {
            Headers = new Dictionary<string, object> { [Headers.RetryCountKey] = 1 }
        };
        var info = new MessageReceivedInfo("ctag", 1UL, false, "orders.topic", "order.created.v1", "orders.queue");

        var ack = await consumer.Handler(payload, props, info, CancellationToken.None);

        ack.Should().Be(AckStrategies.Ack);
        fakeBus.PublishedMessages.Should().ContainSingle();
        var retryPublish = fakeBus.PublishedMessages.Single();
        retryPublish.Exchange.Name.Should().Be(Exchange.Default.Name);
        retryPublish.RoutingKey.Should().Be("orders.queue");
        retryPublish.Mandatory.Should().BeFalse();
        retryPublish.Properties.Type.Should().Be("order.created.v1");
        retryPublish.Properties.Headers[Headers.RetryCountKey].Should().Be(2);
        retryPublish.Body.ToArray().Should().Equal(payload);
    }

    [Fact(DisplayName = "SubscribeAsync_RedeliveredRetryViaDefaultExchange_ShouldPreserveOriginalEventNameFromTypeProperty")]
    public async Task SubscribeAsync_RedeliveredRetryViaDefaultExchange_ShouldPreserveOriginalEventNameFromTypeProperty()
    {
        var fakeBus = new FakeAdvancedBus();
        using var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue"
        });

        EventContext? captured = null;
        subscriber.ConsumerReceived = ctx =>
        {
            captured = ctx;
            return Task.CompletedTask;
        };

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        var consumer = fakeBus.RegisteredConsumers.Single();
        var props = new MessageProperties
        {
            Type = "order.created.v1",
            Headers = new Dictionary<string, object> { [Headers.RetryCountKey] = 1 }
        };
        // Default-exchange retry deliveries carry the queue name as RoutingKey.
        var info = new MessageReceivedInfo("ctag", 2UL, false, "", "orders.queue", "orders.queue");

        var ack = await consumer.Handler(Encoding.UTF8.GetBytes("{}"), props, info, CancellationToken.None);

        ack.Should().Be(AckStrategies.Ack);
        captured.Should().NotBeNull();
        captured!.EventName.Should().Be("order.created.v1");
    }

    [Theory(DisplayName = "SubscribeAsync_HandlerFailsAtMaxRetry_ShouldReturnNackWithoutRequeue")]
    [MemberData(nameof(RetryHeaderEncodingsAtLimit))]
    public async Task SubscribeAsync_HandlerFailsAtMaxRetry_ShouldReturnNackWithoutRequeue(object retryHeaderValue)
    {
        var fakeBus = new FakeAdvancedBus();
        using var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue",
            MaxRetryCount = 3
        });
        subscriber.ConsumerReceived = _ => throw new InvalidOperationException("permanent");

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        var consumer = fakeBus.RegisteredConsumers.Single();
        var props = new MessageProperties
        {
            Headers = new Dictionary<string, object> { [Headers.RetryCountKey] = retryHeaderValue }
        };
        var info = new MessageReceivedInfo("ctag", 1UL, false, "orders.topic", "order.created.v1", "orders.queue");

        var ack = await consumer.Handler(ReadOnlyMemory<byte>.Empty, props, info, CancellationToken.None);

        ack.Should().Be(AckStrategies.NackWithoutRequeue);
        fakeBus.PublishedMessages.Should().BeEmpty();
    }

    [Fact(DisplayName = "Dispose_ShouldDisposeAllCreatedConsumers")]
    public async Task Dispose_ShouldDisposeAllCreatedConsumers()
    {
        var fakeBus = new FakeAdvancedBus();
        var subscriber = CreateSubscriber(fakeBus, new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic",
            GlobalQueue = "orders.queue"
        });

        await subscriber.SubscribeAsync(new List<string> { "order.created.v1" });

        subscriber.Dispose();

        fakeBus.RegisteredConsumers.Should().AllSatisfy(c => c.Handle.IsDisposed.Should().BeTrue());
    }

    public static IEnumerable<object[]> RetryHeaderEncodingsAtLimit()
    {
        yield return new object[] { 3 };
        yield return new object[] { 3L };
        yield return new object[] { Encoding.UTF8.GetBytes("3") };
        yield return new object[] { "3" };
    }

    private static RabbitMqEventSubscriber CreateSubscriber(FakeAdvancedBus fakeBus, RabbitMqBindingOption option)
    {
        return new RabbitMqEventSubscriber(
            () => fakeBus,
            Options.Create(option),
            NullLogger<RabbitMqEventSubscriber>.Instance);
    }
}
