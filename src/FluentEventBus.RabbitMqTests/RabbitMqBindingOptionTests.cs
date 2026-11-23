using FluentAssertions;
using FluentEventBus.RabbitMq;
using Microsoft.Extensions.Options;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqBindingOptionTests
{
    [Fact(DisplayName = "RabbitMqBindingOption_初始化_應創建空字典與預設值")]
    public void RabbitMqBindingOption_Initialized_ShouldCreateEmptyDictionariesAndDefaults()
    {
        var options = new RabbitMqBindingOption();

        options.ExchangeBindings.Should().NotBeNull().And.BeEmpty();
        options.QueueBindings.Should().NotBeNull().And.BeEmpty();
        options.DeadLetterExchangeBindings.Should().NotBeNull().And.BeEmpty();
        options.DeadLetterQueueBindings.Should().NotBeNull().And.BeEmpty();
        options.ServiceName.Should().Be("app");
        options.EnvironmentName.Should().Be("prod");
        options.PrefetchCount.Should().Be(10);
        options.MaxRetryCount.Should().Be(3);
    }

    [Theory(DisplayName = "RabbitMqConnectionProvider_InvalidConnectionOptions_ShouldThrowArgumentException")]
    [InlineData("", "guest", "localhost")]
    [InlineData(" ", "guest", "localhost")]
    [InlineData("guest", "", "localhost")]
    [InlineData("guest", "guest", "")]
    public void RabbitMqConnectionProvider_InvalidConnectionOptions_ShouldThrowArgumentException(
        string userName,
        string password,
        string host)
    {
        var options = Options.Create(new RabbitMqConnectionOption
        {
            UserName = userName,
            Password = password,
            Host = host
        });

        var act = () => new RabbitMqConnectionProvider(options);

        act.Should().Throw<ArgumentException>();
    }

    [Fact(DisplayName = "RabbitMqConnectionProvider_NullOptions_ShouldThrowArgumentNullException")]
    public void RabbitMqConnectionProvider_NullOptions_ShouldThrowArgumentNullException()
    {
        var act = () => new RabbitMqConnectionProvider(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }
}
