using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.ExceptionHandlers;
using FluentEventBus.Profile;
using FluentEventBus.Naming;
using FluentEventBus.Subscriber;

namespace FluentEventBus.Tests;

public class SubscriptionProfileManagerTests
{
    [Fact(DisplayName = "Constructor_WithMultipleProfiles_ShouldRegisterEachEventOnce")]
    public void Constructor_WithMultipleProfiles_ShouldRegisterEachEventOnce()
    {
        var profiles = new SubscriptionProfile[]
        {
            new ProfileA(),
            new ProfileB()
        };

        var mapper = new TestEventNameRegistry();
        var manager = new SubscriptionProfileManager(profiles, mapper);

        mapper.RegisteredTypes.Should().BeEquivalentTo(new[] { typeof(EventA), typeof(EventB) });
        manager.HasSubscriptionsForEvent(typeof(EventA)).Should().BeTrue();
        manager.HasSubscriptionsForEvent(typeof(EventB)).Should().BeTrue();
    }

    [Fact(DisplayName = "Constructor_WithOverlappingProfilesForSameEvent_ShouldAggregateHandlersAndRegisterOnce")]
    public void Constructor_WithOverlappingProfilesForSameEvent_ShouldAggregateHandlersAndRegisterOnce()
    {
        var profiles = new SubscriptionProfile[]
        {
            new ProfileA(),
            new ProfileAAlso()
        };

        var mapper = new TestEventNameRegistry();
        var manager = new SubscriptionProfileManager(profiles, mapper);

        mapper.RegisteredTypes.Should().ContainSingle().Which.Should().Be(typeof(EventA));
        manager.GetAllEventTypes().Should().ContainSingle().Which.Should().Be(typeof(EventA));
        manager.GetEventHandlerExecutorsForEvent(typeof(EventA))
            .Select(e => e.HandlerType)
            .Should().BeEquivalentTo(new[] { typeof(HandlerA), typeof(HandlerA2) });
        manager.GetAllSubscriptions()[typeof(EventA)].Should().HaveCount(2);
    }

    [Fact(DisplayName = "Constructor_ShouldExposeErrorHandlersAndSubscriptionsImmediately")]
    public void Constructor_ShouldExposeErrorHandlersAndSubscriptionsImmediately()
    {
        var profiles = new SubscriptionProfile[]
        {
            new ProfileWithErrorHandler()
        };

        var mapper = new TestEventNameRegistry();
        var manager = new SubscriptionProfileManager(profiles, mapper);

        manager.HasSubscriptionsForEvent(typeof(EventA)).Should().BeTrue();
        manager.HasSubscriptionsForEvent(typeof(EventB)).Should().BeFalse();
        manager.GetErrorHandlersForEvent(typeof(EventA)).Should().ContainSingle().Which.Should().Be(typeof(TestErrorHandler));
        manager.GetErrorHandlersForEvent(typeof(EventB)).Should().BeEmpty();
        manager.GetAllSubscriptions().Should().ContainKey(typeof(EventA));
    }

    [Fact(DisplayName = "Constructor_WithNullArguments_ShouldThrowArgumentNullException")]
    public void Constructor_WithNullArguments_ShouldThrowArgumentNullException()
    {
        var mapper = new TestEventNameRegistry();
        var profiles = Array.Empty<SubscriptionProfile>();

        var actNullProfiles = () => new SubscriptionProfileManager(null!, mapper);
        var actNullMapper = () => new SubscriptionProfileManager(profiles, null!);

        actNullProfiles.Should().Throw<ArgumentNullException>().WithParameterName("profiles");
        actNullMapper.Should().Throw<ArgumentNullException>().WithParameterName("eventMapper");
    }

    private sealed class ProfileA : SubscriptionProfile
    {
        public ProfileA()
        {
            this.WhenOccurs<EventA>().ToDo<HandlerA>();
        }
    }

    private sealed class ProfileAAlso : SubscriptionProfile
    {
        public ProfileAAlso()
        {
            this.WhenOccurs<EventA>().ToDo<HandlerA2>();
        }
    }

    private sealed class ProfileB : SubscriptionProfile
    {
        public ProfileB()
        {
            this.WhenOccurs<EventB>().ToDo<HandlerB>();
        }
    }

    private sealed class ProfileWithErrorHandler : SubscriptionProfile
    {
        public ProfileWithErrorHandler()
        {
            this.WhenOccurs<EventA>().ToDo<HandlerA>().CatchExceptionToDo<TestErrorHandler>();
        }
    }

    private sealed class HandlerA : IEventHandler<EventA>
    {
        public Task HandleAsync(EventA @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class HandlerA2 : IEventHandler<EventA>
    {
        public Task HandleAsync(EventA @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class HandlerB : IEventHandler<EventB>
    {
        public Task HandleAsync(EventB @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestErrorHandler : IEventExceptionHandler
    {
        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed record EventA(Guid Id);
    private sealed record EventB(Guid Id);

    private sealed class TestEventNameRegistry : IEventNameRegistry
    {
        private readonly HashSet<Type> _types = new();

        public IReadOnlyCollection<Type> RegisteredTypes => _types;

        public void Register(Type eventType)
        {
            if (!_types.Add(eventType))
            {
                throw new ArgumentException($"Duplicate registration for {eventType.FullName}.");
            }
        }

        public string GetEventName(Type type) => type.Name;

        public Type GetEventType(string type)
        {
            return _types.First(t => t.Name == type);
        }
    }
}
