using EasyNetQ;
using EasyNetQ.Persistent;
using EasyNetQ.Producer;
using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.Naming;
using FluentEventBus.RabbitMq;
using FluentEventBus.RabbitMqTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;
using ShutdownEventArgs = RabbitMQ.Client.ShutdownEventArgs;
using ShutdownInitiator = RabbitMQ.Client.ShutdownInitiator;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqEventPublisherTests
{
    [Fact(DisplayName = "PublishAsync_DefaultOptions_ShouldPublishWithMandatoryTrueAndCopyHeaders")]
    public async Task PublishAsync_DefaultOptions_ShouldPublishWithMandatoryTrueAndCopyHeaders()
    {
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, properties, _, _) =>
            {
                // EasyNetQ's confirmation pipeline injects internal headers into properties.Headers.
                properties.Headers["easynetq-confirmation-id"] = 42UL;
                return Task.CompletedTask;
            }
        };
        using var publisher = CreatePublisher(fakeBus);
        var callerHeaders = new Headers { CorrelationId = "corr-1" };

        await publisher.PublishAsync(new SampleOrderEvent("ok"), callerHeaders);

        fakeBus.LastMandatory.Should().BeTrue();
        fakeBus.LastExchange.Name.Should().Be("orders.topic");
        fakeBus.LastRoutingKey.Should().Be(nameof(SampleOrderEvent));
        fakeBus.LastProperties.Should().NotBeNull();
        fakeBus.LastProperties!.DeliveryMode.Should().Be(DeliveryMode.Persistent);
        callerHeaders.Should().NotContainKey("easynetq-confirmation-id");
    }

    [Fact(DisplayName = "PublishAsync_MandatoryDisabled_ShouldPublishWithMandatoryFalse")]
    public async Task PublishAsync_MandatoryDisabled_ShouldPublishWithMandatoryFalse()
    {
        var fakeBus = new FakeAdvancedBus();
        var options = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = false,
            ConfirmTimeout = TimeSpan.FromSeconds(5)
        };
        using var publisher = CreatePublisher(fakeBus, options);

        await publisher.PublishAsync(new SampleOrderEvent("ok"));

        fakeBus.LastMandatory.Should().BeFalse();
    }

    [Fact(DisplayName = "PublishAsync_BrokerNacks_ShouldThrowBrokerRejectedException")]
    public async Task PublishAsync_BrokerNacks_ShouldThrowBrokerRejectedException()
    {
        var nacked = new PublishNackedException("Broker has signalled that publish 1 is nacked");
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, _, _, _) => Task.FromException(nacked)
        };
        using var publisher = CreatePublisher(fakeBus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("nack"));

        var assertion = await act.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.BrokerRejected);
        assertion.Which.Exchange.Should().Be("orders.topic");
        assertion.Which.RoutingKey.Should().Be(nameof(SampleOrderEvent));
        assertion.Which.InnerException.Should().BeSameAs(nacked);
    }

    [Fact(DisplayName = "PublishAsync_UnroutableMessage_PublishReturnedException_ShouldThrowMessageReturnedExceptionWithReason")]
    public async Task PublishAsync_UnroutableMessage_PublishReturnedException_ShouldThrowMessageReturnedExceptionWithReason()
    {
        var returnedEx = new PublishReturnedException("Broker has signalled that message is returned");
        FakeAdvancedBus? bus = null;
        bus = new FakeAdvancedBus
        {
            OnPublish = (exchange, routingKey, _, properties, body, _) =>
            {
                bus!.RaiseMessageReturned(new MessageReturnedEventArgs(
                    body,
                    properties,
                    new MessageReturnedInfo(exchange.Name, routingKey, "NO_ROUTE")));
                return Task.FromException(returnedEx);
            }
        };
        using var publisher = CreatePublisher(bus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("unroutable"));

        var assertion = await act.Should().ThrowAsync<RabbitMqMessageReturnedException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.Unroutable);
        assertion.Which.ReturnReason.Should().Be("NO_ROUTE");
        assertion.Which.Exchange.Should().Be("orders.topic");
        assertion.Which.RoutingKey.Should().Be(nameof(SampleOrderEvent));
        assertion.Which.InnerException.Should().BeSameAs(returnedEx);
    }

    [Fact(DisplayName = "PublishAsync_UnroutableMessage_MessageReturnedFiresAfterPublishReturnedException_ShouldStillCaptureReturnReason")]
    public async Task PublishAsync_UnroutableMessage_MessageReturnedFiresAfterPublishReturnedException_ShouldStillCaptureReturnReason()
    {
        var returnedEx = new PublishReturnedException("Broker has signalled that message is returned");
        FakeAdvancedBus? bus = null;
        bus = new FakeAdvancedBus
        {
            OnPublish = (exchange, routingKey, _, properties, body, _) =>
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(10);
                    bus!.RaiseMessageReturned(new MessageReturnedEventArgs(
                        body,
                        properties,
                        new MessageReturnedInfo(exchange.Name, routingKey, "NO_ROUTE")));
                });
                return Task.FromException(returnedEx);
            }
        };
        using var publisher = CreatePublisher(bus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("unroutable"));

        var assertion = await act.Should().ThrowAsync<RabbitMqMessageReturnedException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.Unroutable);
        assertion.Which.ReturnReason.Should().Be("NO_ROUTE");
    }

    [Fact(DisplayName = "PublishAsync_UnroutableMessage_SynchronousMessageReturnedWithoutThrow_ShouldNotReportSuccess")]
    public async Task PublishAsync_UnroutableMessage_SynchronousMessageReturnedWithoutThrow_ShouldNotReportSuccess()
    {
        FakeAdvancedBus? bus = null;
        bus = new FakeAdvancedBus
        {
            OnPublish = (exchange, routingKey, _, properties, body, _) =>
            {
                bus!.RaiseMessageReturned(new MessageReturnedEventArgs(
                    body,
                    properties,
                    new MessageReturnedInfo(exchange.Name, routingKey, "NO_ROUTE")));
                return Task.CompletedTask;
            }
        };
        using var publisher = CreatePublisher(bus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("unroutable"));

        var assertion = await act.Should().ThrowAsync<RabbitMqMessageReturnedException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.Unroutable);
        assertion.Which.ReturnReason.Should().Be("NO_ROUTE");
    }

    [Fact(DisplayName = "PublishAsync_ConfirmTimeoutExpires_ShouldThrowPublishTimeoutException")]
    public async Task PublishAsync_ConfirmTimeoutExpires_ShouldThrowPublishTimeoutException()
    {
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = async (_, _, _, _, _, ct) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
        };
        var timeout = TimeSpan.FromMilliseconds(50);
        using var publisher = CreatePublisher(fakeBus, new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = true,
            ConfirmTimeout = timeout
        });

        var act = () => publisher.PublishAsync(new SampleOrderEvent("slow"));

        var assertion = await act.Should().ThrowAsync<RabbitMqPublishTimeoutException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ConfirmTimeout);
        assertion.Which.Timeout.Should().Be(timeout);
        assertion.Which.Exchange.Should().Be("orders.topic");
        assertion.Which.RoutingKey.Should().Be(nameof(SampleOrderEvent));
    }

    [Fact(DisplayName = "PublishAsync_CallerCancelsToken_ShouldPropagateOperationCanceledException")]
    public async Task PublishAsync_CallerCancelsToken_ShouldPropagateOperationCanceledException()
    {
        var publishStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = async (_, _, _, _, _, ct) =>
            {
                publishStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
        };
        using var publisher = CreatePublisher(fakeBus, new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = true,
            ConfirmTimeout = TimeSpan.FromSeconds(10)
        });
        using var callerCts = new CancellationTokenSource();

        var publishTask = publisher.PublishAsync(new SampleOrderEvent("cancelled"), cancellationToken: callerCts.Token);
        await publishStarted.Task;
        callerCts.Cancel();

        var act = () => publishTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact(DisplayName = "PublishAsync_ChannelShutdown_ShouldThrowChannelShutdownAndEvictExchangeCache")]
    public async Task PublishAsync_ChannelShutdown_ShouldThrowChannelShutdownAndEvictExchangeCache()
    {
        var attempt = 0;
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, _, _, _) =>
            {
                if (Interlocked.Increment(ref attempt) == 1)
                {
                    var args = new ShutdownEventArgs(ShutdownInitiator.Peer, 404, "NOT_FOUND - no exchange");
                    return Task.FromException(new AlreadyClosedException(args));
                }

                return Task.CompletedTask;
            }
        };
        using var publisher = CreatePublisher(fakeBus);

        var firstAct = () => publisher.PublishAsync(new SampleOrderEvent("first"));
        var assertion = await firstAct.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ChannelShutdown);

        await publisher.PublishAsync(new SampleOrderEvent("second"));

        fakeBus.ExchangeDeclareCount.Should().Be(2);
    }

    [Fact(DisplayName = "PublishAsync_ConnectionClosedShutdownReason_ShouldThrowConnectionInterrupted")]
    public async Task PublishAsync_ConnectionClosedShutdownReason_ShouldThrowConnectionInterrupted()
    {
        var shutdown = new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED", 10, 50, null!);
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, _, _, _) => Task.FromException(new OperationInterruptedException(shutdown))
        };
        using var publisher = CreatePublisher(fakeBus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("conn-closed"));

        var assertion = await act.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ConnectionInterrupted);
    }

    [Fact(DisplayName = "PublishAsync_PublishInterruptedException_ShouldThrowConnectionInterrupted")]
    public async Task PublishAsync_PublishInterruptedException_ShouldThrowConnectionInterrupted()
    {
        var interrupted = new PublishInterruptedException("Channel recovered");
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, _, _, _) => Task.FromException(interrupted)
        };
        using var publisher = CreatePublisher(fakeBus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("interrupted"));

        var assertion = await act.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ConnectionInterrupted);
        assertion.Which.InnerException.Should().BeSameAs(interrupted);
    }

    [Fact(DisplayName = "PublishAsync_BrokerUnreachableException_ShouldThrowConnectionInterrupted")]
    public async Task PublishAsync_BrokerUnreachableException_ShouldThrowConnectionInterrupted()
    {
        var unreachable = new BrokerUnreachableException(new InvalidOperationException("No host reachable"));
        var fakeBus = new FakeAdvancedBus
        {
            OnPublish = (_, _, _, _, _, _) => Task.FromException(unreachable)
        };
        using var publisher = CreatePublisher(fakeBus);

        var act = () => publisher.PublishAsync(new SampleOrderEvent("unreachable"));

        var assertion = await act.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ConnectionInterrupted);
        assertion.Which.InnerException.Should().BeSameAs(unreachable);
    }

    [Fact(DisplayName = "PublishAsync_ProducerDisconnectedDuringConfirmWait_ShouldThrowConnectionInterruptedAndEvictExchangeCache")]
    public async Task PublishAsync_ProducerDisconnectedDuringConfirmWait_ShouldThrowConnectionInterruptedAndEvictExchangeCache()
    {
        var attempt = 0;
        FakeAdvancedBus? bus = null;
        bus = new FakeAdvancedBus
        {
            OnPublish = async (_, _, _, _, _, ct) =>
            {
                if (Interlocked.Increment(ref attempt) == 1)
                {
                    bus!.RaiseDisconnected(new DisconnectedEventArgs(
                        PersistentConnectionType.Producer,
                        "localhost",
                        5672,
                        "Socket closed"));
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
            }
        };
        using var publisher = CreatePublisher(bus, new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = true,
            ConfirmTimeout = TimeSpan.FromMilliseconds(60)
        });

        var firstAct = () => publisher.PublishAsync(new SampleOrderEvent("disconnect"));
        var assertion = await firstAct.Should().ThrowAsync<RabbitMqPublishException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.ConnectionInterrupted);
        assertion.Which.Should().NotBeOfType<RabbitMqPublishTimeoutException>();

        await publisher.PublishAsync(new SampleOrderEvent("after-reconnect"));

        bus.ExchangeDeclareCount.Should().Be(2);
    }

    [Fact(DisplayName = "Dispose_ShouldUnsubscribeBusEvents")]
    public async Task Dispose_ShouldUnsubscribeBusEvents()
    {
        var fakeBus = new FakeAdvancedBus();
        var publisher = CreatePublisher(fakeBus);

        await publisher.PublishAsync(new SampleOrderEvent("warmup"));
        fakeBus.MessageReturnedSubscriberCount.Should().Be(1);
        fakeBus.DisconnectedSubscriberCount.Should().Be(1);

        publisher.Dispose();

        fakeBus.MessageReturnedSubscriberCount.Should().Be(0);
        fakeBus.DisconnectedSubscriberCount.Should().Be(0);

        var act = () =>
        {
            fakeBus.RaiseMessageReturned(new MessageReturnedEventArgs(
                ReadOnlyMemory<byte>.Empty,
                new MessageProperties(),
                new MessageReturnedInfo("orders.topic", nameof(SampleOrderEvent), "NO_ROUTE")));
            fakeBus.RaiseDisconnected(new DisconnectedEventArgs(
                PersistentConnectionType.Producer,
                "localhost",
                5672,
                "Disposed"));
        };
        act.Should().NotThrow();
    }

    private static RabbitMqEventPublisher CreatePublisher(
        FakeAdvancedBus fakeBus,
        RabbitMqPublishOptions? publishOptions = null)
    {
        var bindingOptions = Options.Create(new RabbitMqBindingOption
        {
            GlobalExchange = "orders.topic"
        });

        return new RabbitMqEventPublisher(
            new FluentEventBus.Serialization.JsonSerializer(),
            new StubEventNameRegistry(),
            () => fakeBus,
            bindingOptions,
            Options.Create(publishOptions ?? new RabbitMqPublishOptions()),
            NullLogger<RabbitMqEventPublisher>.Instance);
    }

    private sealed record SampleOrderEvent(string Message);

    private sealed class StubEventNameRegistry : IEventNameRegistry
    {
        public void Register(Type eventType) { }

        public string GetEventName(Type type) => type.Name;

        public Type GetEventType(string type) => typeof(SampleOrderEvent);
    }
}
