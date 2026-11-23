using System.Text;
using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.Internal;
using FluentEventBus.Naming;
using FluentEventBus.Profile;
using FluentEventBus.Serialization;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace FluentEventBus.Tests.Internal;

public class BootstrapperAndInitializerTests
{
    [Fact(DisplayName = "DefaultApplicationBootstrapper_StartAndStop_ShouldRunInitializersAndTerminatorsInOrder")]
    public async Task DefaultApplicationBootstrapper_StartAndStop_ShouldRunInitializersAndTerminatorsInOrder()
    {
        var log = new List<string>();
        var initializers = new IInitializer[]
        {
            new RecordingInitializer("init-1", log),
            new RecordingInitializer("init-2", log)
        };
        var terminators = new ITerminator[]
        {
            new RecordingTerminator("term-1", log),
            new RecordingTerminator("term-2", log)
        };
        var bootstrapper = new DefaultApplicationBootstrapper(initializers, terminators);

        await bootstrapper.StartAsync(CancellationToken.None);
        await bootstrapper.StopAsync(CancellationToken.None);

        log.Should().Equal("init-1", "init-2", "term-1", "term-2");
    }

    [Fact(DisplayName = "EventSubscribeInitializer_WithJsonSerializer_ShouldSubscribeAndDispatchReceivedEvent")]
    public async Task EventSubscribeInitializer_WithJsonSerializer_ShouldSubscribeAndDispatchReceivedEvent()
    {
        var probe = new Probe();
        var subscriber = new StubEventSubscriber();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(probe);
        services.AddScoped<TestPingHandler>();
        services.AddSingleton<IEventSubscriber>(subscriber);
        services.AddEventBus(builder => builder.WithProfile<TestPingProfile>());

        await using var provider = services.BuildServiceProvider();
        var initializer = provider.GetRequiredService<IEnumerable<IInitializer>>()
            .OfType<EventSubscribeInitializer>()
            .Single();
        var registry = provider.GetRequiredService<IEventNameRegistry>();
        var serializer = provider.GetRequiredService<ISerializer>();

        await initializer.InitializeAsync(CancellationToken.None);

        var expectedName = registry.GetEventName(typeof(TestPingEvent));
        subscriber.SubscribedEventNames.Should().ContainSingle().Which.Should().Be(expectedName);
        subscriber.ConsumerReceived.Should().NotBeNull();

        var @event = new TestPingEvent(Guid.NewGuid(), "hello");
        var headers = new Headers { CorrelationId = "corr-1" };
        await subscriber.ConsumerReceived!(new EventContext(serializer.Serialize(@event), headers, expectedName));

        probe.ReceivedEvent.Should().Be(@event);
        probe.ReceivedHeaders.Should().BeSameAs(headers);
    }

    [Fact(DisplayName = "EventSubscribeInitializer_WithCustomSerializer_ShouldUseStringDeserializeFallback")]
    public async Task EventSubscribeInitializer_WithCustomSerializer_ShouldUseStringDeserializeFallback()
    {
        var probe = new Probe();
        var subscriber = new StubEventSubscriber();
        var customSerializer = new CustomStringSerializer();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(probe);
        services.AddScoped<TestPingHandler>();
        services.AddEventBus(builder => builder.WithProfile<TestPingProfile>());
        services.AddSingleton<ISerializer>(customSerializer);

        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<IEventNameRegistry>();
        var profileManager = provider.GetRequiredService<ISubscriptionProfileManager>();
        var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();

        var initializer = new EventSubscribeInitializer(
            subscriber,
            profileManager,
            NullLogger<EventSubscribeInitializer>.Instance,
            scopeFactory,
            registry);

        await initializer.InitializeAsync(CancellationToken.None);

        var expectedName = registry.GetEventName(typeof(TestPingEvent));
        var payload = new TestPingEvent(Guid.NewGuid(), "custom");
        var raw = Encoding.UTF8.GetBytes($"{payload.Id}|{payload.Text}");

        await subscriber.ConsumerReceived!(new EventContext(raw, new Headers(), expectedName));

        customSerializer.UsedStringDeserialize.Should().BeTrue();
        probe.ReceivedEvent.Should().Be(payload);
    }

    [Fact(DisplayName = "EventSubscribeInitializer_WhenDeserializedEventIsNull_ShouldThrowInvalidOperationException")]
    public async Task EventSubscribeInitializer_WhenDeserializedEventIsNull_ShouldThrowInvalidOperationException()
    {
        var subscriber = new StubEventSubscriber();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddScoped<TestPingHandler>();
        services.AddSingleton(new Probe());
        services.AddEventBus(builder => builder.WithProfile<TestPingProfile>());
        services.AddSingleton<ISerializer>(new NullReturningSerializer());

        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<IEventNameRegistry>();
        var initializer = new EventSubscribeInitializer(
            subscriber,
            provider.GetRequiredService<ISubscriptionProfileManager>(),
            NullLogger<EventSubscribeInitializer>.Instance,
            provider.GetRequiredService<IServiceScopeFactory>(),
            registry);

        await initializer.InitializeAsync(CancellationToken.None);
        var expectedName = registry.GetEventName(typeof(TestPingEvent));

        var act = () => subscriber.ConsumerReceived!(new EventContext(Encoding.UTF8.GetBytes("null"), new Headers(), expectedName));

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    private sealed record TestPingEvent(Guid Id, string Text);

    private sealed class Probe
    {
        public TestPingEvent? ReceivedEvent { get; set; }
        public Headers? ReceivedHeaders { get; set; }
    }

    private sealed class TestPingHandler : IEventHandler<TestPingEvent>
    {
        private readonly Probe _probe;
        public TestPingHandler(Probe probe) => _probe = probe;

        public Task HandleAsync(TestPingEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.ReceivedEvent = @event;
            _probe.ReceivedHeaders = headers;
            return Task.CompletedTask;
        }
    }

    private sealed class TestPingProfile : SubscriptionProfile
    {
        public TestPingProfile() => this.WhenOccurs<TestPingEvent>().ToDo<TestPingHandler>();
    }

    private sealed class StubEventSubscriber : IEventSubscriber
    {
        public List<string> SubscribedEventNames { get; private set; } = new();
        public Func<EventContext, Task>? ConsumerReceived { get; set; }

        public Task SubscribeAsync(List<string> eventNames)
        {
            SubscribedEventNames = eventNames;
            return Task.CompletedTask;
        }
    }

    private sealed class CustomStringSerializer : ISerializer
    {
        public bool UsedStringDeserialize { get; private set; }

        public ReadOnlyMemory<byte> Serialize<TEvent>(TEvent @event)
            => Encoding.UTF8.GetBytes(@event!.ToString()!);

        public object? Deserialize(string content, Type type)
        {
            UsedStringDeserialize = true;
            var parts = content.Split('|');
            return new TestPingEvent(Guid.Parse(parts[0]), parts[1]);
        }

        public object? Deserialize(ReadOnlyMemory<byte> data, Type type)
            => throw new NotSupportedException("Should fall back to string overload for non-JsonSerializer.");
    }

    private sealed class NullReturningSerializer : ISerializer
    {
        public ReadOnlyMemory<byte> Serialize<TEvent>(TEvent @event) => ReadOnlyMemory<byte>.Empty;
        public object? Deserialize(string content, Type type) => null;
        public object? Deserialize(ReadOnlyMemory<byte> data, Type type) => null;
    }

    private sealed class RecordingInitializer : IInitializer
    {
        private readonly string _name;
        private readonly List<string> _log;

        public RecordingInitializer(string name, List<string> log)
        {
            _name = name;
            _log = log;
        }

        public Task InitializeAsync(CancellationToken cancellationToken)
        {
            _log.Add(_name);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingTerminator : ITerminator
    {
        private readonly string _name;
        private readonly List<string> _log;

        public RecordingTerminator(string name, List<string> log)
        {
            _name = name;
            _log = log;
        }

        public Task TerminateAsync(CancellationToken cancellationToken)
        {
            _log.Add(_name);
            return Task.CompletedTask;
        }
    }
}
