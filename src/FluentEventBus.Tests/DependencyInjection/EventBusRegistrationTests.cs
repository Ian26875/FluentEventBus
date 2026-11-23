using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.ExceptionHandlers;
using FluentEventBus.Internal;
using FluentEventBus.Naming;
using FluentEventBus.Profile;
using FluentEventBus.Serialization;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FluentEventBus.Tests.DependencyInjection;

public class EventBusRegistrationTests
{
    [Fact(DisplayName = "ScanHandlersFrom_ShouldRegisterConcreteHandlersAsScopedAndSkipAbstractOrOpenGeneric")]
    public void ScanHandlersFrom_ShouldRegisterConcreteHandlersAsScopedAndSkipAbstractOrOpenGeneric()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(builder =>
        {
            builder.ScanHandlersFrom<EventBusRegistrationTests>();
        });

        services.Should().Contain(d =>
            d.ServiceType == typeof(IEventHandler<ScanSampleEvent>) &&
            d.ImplementationType == typeof(PublicScanSampleHandler) &&
            d.Lifetime == ServiceLifetime.Scoped);

        services.Should().Contain(d =>
            d.ServiceType == typeof(PublicScanSampleHandler) &&
            d.ImplementationType == typeof(PublicScanSampleHandler) &&
            d.Lifetime == ServiceLifetime.Scoped);

        services.Should().Contain(d =>
            d.ServiceType == typeof(IEventExceptionHandler) &&
            d.ImplementationType == typeof(PublicScanErrorHandler) &&
            d.Lifetime == ServiceLifetime.Scoped);

        services.Should().NotContain(d => d.ImplementationType == typeof(AbstractScanSampleHandler));
        services.Should().NotContain(d => d.ImplementationType == typeof(OpenGenericScanHandler<>));
    }

    [Fact(DisplayName = "AddEventBusWithProfile_ShouldRegisterProfileHandlersAndCoreServices")]
    public async Task AddEventBusWithProfile_ShouldRegisterProfileHandlersAndCoreServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IEventSubscriber>(new NoopEventSubscriber());
        services.AddEventBusWithProfile<PublicScanProfile, EventBusRegistrationTests>();

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<ISubscriptionProfileManager>()
            .HasSubscriptionsForEvent(typeof(ScanSampleEvent))
            .Should().BeTrue();
        provider.GetRequiredService<IEventNameRegistry>().Should().BeOfType<EventNameRegistry>();
        provider.GetRequiredService<IEventHandlerInvoker>().Should().NotBeNull();
        provider.GetRequiredService<ISerializer>().Should().BeOfType<JsonSerializer>();
        provider.GetRequiredService<IApplicationBootstrapper>().Should().BeOfType<DefaultApplicationBootstrapper>();
        provider.GetServices<IHostedService>().OfType<DefaultApplicationBootstrapper>().Should().ContainSingle();

        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetRequiredService<PublicScanSampleHandler>().Should().NotBeNull();
    }

    public sealed record ScanSampleEvent(string Name);

    public sealed class PublicScanSampleHandler : IEventHandler<ScanSampleEvent>
    {
        public Task HandleAsync(ScanSampleEvent @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public sealed class PublicScanErrorHandler : IEventExceptionHandler
    {
        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public abstract class AbstractScanSampleHandler : IEventHandler<ScanSampleEvent>
    {
        public abstract Task HandleAsync(ScanSampleEvent @event, Headers headers, CancellationToken cancellationToken);
    }

    public class OpenGenericScanHandler<T> : IEventHandler<T> where T : class
    {
        public Task HandleAsync(T @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public sealed class PublicScanProfile : SubscriptionProfile
    {
        public PublicScanProfile() => this.WhenOccurs<ScanSampleEvent>().ToDo<PublicScanSampleHandler>();
    }

    private sealed class NoopEventSubscriber : IEventSubscriber
    {
        public Func<EventContext, Task>? ConsumerReceived { get; set; }
        public Task SubscribeAsync(List<string> eventNames) => Task.CompletedTask;
    }
}
