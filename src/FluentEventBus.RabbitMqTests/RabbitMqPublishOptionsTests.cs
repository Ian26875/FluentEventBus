using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqPublishOptionsTests
{
    [Fact(DisplayName = "RabbitMqPublishOptions_Defaults_ShouldEnableConfirmsAndMandatoryWithTenSecondTimeout")]
    public void RabbitMqPublishOptions_Defaults_ShouldEnableConfirmsAndMandatoryWithTenSecondTimeout()
    {
        var options = new RabbitMqPublishOptions();

        options.PublisherConfirmEnabled.Should().BeTrue();
        options.Mandatory.Should().BeTrue();
        options.ConfirmTimeout.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact(DisplayName = "BuildConnectionConfiguration_ShouldApplyConfirmAndMandatoryFlagsAndHostPort")]
    public void BuildConnectionConfiguration_ShouldApplyConfirmAndMandatoryFlagsAndHostPort()
    {
        var connectionOption = new RabbitMqConnectionOption
        {
            UserName = "guest",
            Password = "guest",
            Host = "rabbitmq.local:5673"
        };
        var publishOption = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = true,
            ConfirmTimeout = TimeSpan.FromSeconds(25)
        };

        var config = RabbitMqConnectionProvider.BuildConnectionConfiguration(connectionOption, publishOption);

        config.PublisherConfirms.Should().BeTrue();
        config.MandatoryPublish.Should().BeTrue();
        config.Timeout.Should().Be(TimeSpan.FromSeconds(25));
        config.Hosts.Should().ContainSingle();
        config.Hosts[0].Host.Should().Be("rabbitmq.local");
        config.Hosts[0].Port.Should().Be(5673);
    }

    [Fact(DisplayName = "BuildConnectionConfiguration_WithShorterConfirmTimeout_ShouldKeepDefaultBusTimeout")]
    public void BuildConnectionConfiguration_WithShorterConfirmTimeout_ShouldKeepDefaultBusTimeout()
    {
        var connectionOption = new RabbitMqConnectionOption
        {
            UserName = "guest",
            Password = "guest",
            Host = "localhost"
        };
        var publishOption = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            Mandatory = false,
            ConfirmTimeout = TimeSpan.FromSeconds(2)
        };

        var config = RabbitMqConnectionProvider.BuildConnectionConfiguration(connectionOption, publishOption);

        config.PublisherConfirms.Should().BeTrue();
        config.MandatoryPublish.Should().BeFalse();
        config.Timeout.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact(DisplayName = "Validate_WithZeroTimeoutWhenConfirmsEnabled_ShouldThrowArgumentOutOfRangeException")]
    public void Validate_WithZeroTimeoutWhenConfirmsEnabled_ShouldThrowArgumentOutOfRangeException()
    {
        var options = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = true,
            ConfirmTimeout = TimeSpan.Zero
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqPublishOptions.ConfirmTimeout));
    }

    [Fact(DisplayName = "Validate_WithNegativeTimeout_ShouldThrowArgumentOutOfRangeException")]
    public void Validate_WithNegativeTimeout_ShouldThrowArgumentOutOfRangeException()
    {
        var options = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = false,
            ConfirmTimeout = TimeSpan.FromMilliseconds(-1)
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(RabbitMqPublishOptions.ConfirmTimeout));
    }

    [Fact(DisplayName = "Validate_WithZeroTimeoutWhenConfirmsDisabled_ShouldNotThrow")]
    public void Validate_WithZeroTimeoutWhenConfirmsDisabled_ShouldNotThrow()
    {
        var options = new RabbitMqPublishOptions
        {
            PublisherConfirmEnabled = false,
            ConfirmTimeout = TimeSpan.Zero
        };

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Fact(DisplayName = "UseRabbitMqTransport_TwoArgOverload_ShouldRegisterDefaultPublishOptions")]
    public void UseRabbitMqTransport_TwoArgOverload_ShouldRegisterDefaultPublishOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(builder =>
        {
            builder.UseRabbitMqTransport(
                opt =>
                {
                    opt.Host = "localhost";
                    opt.UserName = "guest";
                    opt.Password = "guest";
                },
                bind => bind.GlobalExchange = "eventbus.topic");
        });

        using var provider = services.BuildServiceProvider();
        var publishOptions = provider.GetRequiredService<IOptions<RabbitMqPublishOptions>>().Value;

        publishOptions.PublisherConfirmEnabled.Should().BeTrue();
        publishOptions.Mandatory.Should().BeTrue();
        publishOptions.ConfirmTimeout.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact(DisplayName = "UseRabbitMqTransport_ThreeArgOverload_ShouldConfigureCustomPublishOptions")]
    public void UseRabbitMqTransport_ThreeArgOverload_ShouldConfigureCustomPublishOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(builder =>
        {
            builder.UseRabbitMqTransport(
                opt =>
                {
                    opt.Host = "localhost";
                    opt.UserName = "guest";
                    opt.Password = "guest";
                },
                bind => bind.GlobalExchange = "eventbus.topic",
                publish =>
                {
                    publish.PublisherConfirmEnabled = false;
                    publish.Mandatory = false;
                    publish.ConfirmTimeout = TimeSpan.FromSeconds(30);
                });
        });

        using var provider = services.BuildServiceProvider();
        var publishOptions = provider.GetRequiredService<IOptions<RabbitMqPublishOptions>>().Value;

        publishOptions.PublisherConfirmEnabled.Should().BeFalse();
        publishOptions.Mandatory.Should().BeFalse();
        publishOptions.ConfirmTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }
}
