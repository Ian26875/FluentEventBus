using FluentAssertions;
using FluentEventBus.Naming;
using FluentEventBus.RabbitMq;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqBindingBuilderExtensionsTests
{
    [Fact(DisplayName = "ForEvent_為指定事件創建EventBinder_應返回EventBinder實例")]
    public void ForEvent_ShouldReturnEventBinderInstanceForGivenEvent()
    {
        var options = new RabbitMqBindingOption();

        var binder = options.ForEvent<TestEvent>();

        binder.Should().NotBeNull();
        binder.Should().BeOfType<EventBinder<TestEvent>>();
    }

    [Fact(DisplayName = "DeclareGlobalMethods_ShouldSetGlobalExchangeQueueAndDeadLetter")]
    public void DeclareGlobalMethods_ShouldSetGlobalExchangeQueueAndDeadLetter()
    {
        var options = new RabbitMqBindingOption();

        options
            .DeclareGlobalExchange("global.exchange")
            .DeclareGlobalQueue("global.queue")
            .DeclareGlobalDeadLetter("global.dlx", "global.dlq");

        options.GlobalExchange.Should().Be("global.exchange");
        options.GlobalQueue.Should().Be("global.queue");
        options.GlobalDeadLetterExchange.Should().Be("global.dlx");
        options.GlobalDeadLetterQueue.Should().Be("global.dlq");
    }

    [Fact(DisplayName = "ForEvent_FluentConfiguration_ShouldPopulateAllEventKeyedBindings")]
    public void ForEvent_FluentConfiguration_ShouldPopulateAllEventKeyedBindings()
    {
        var registry = new EventNameRegistry();
        var expectedEventName = registry.GetEventName(typeof(TestEvent));
        var options = new RabbitMqBindingOption
        {
            EventNameRegistry = registry
        };

        options.ForEvent<TestEvent>()
            .DeclareExchange("custom.exchange")
            .DeclareQueue("custom.queue")
            .WithDeadLetter("custom.dlx", "custom.dlq");

        options.ExchangeBindings[expectedEventName].Should().Be("custom.exchange");
        options.QueueBindings[expectedEventName].Should().Be("custom.queue");
        options.DeadLetterExchangeBindings[expectedEventName].Should().Be("custom.dlx");
        options.DeadLetterQueueBindings[expectedEventName].Should().Be("custom.dlq");
    }
}
