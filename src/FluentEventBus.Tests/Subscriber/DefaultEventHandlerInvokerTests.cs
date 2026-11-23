using System.Collections.Concurrent;
using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Errors;
using FluentEventBus.Event;
using FluentEventBus.ExceptionHandlers;
using FluentEventBus.Profile;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;

namespace FluentEventBus.Tests.Subscriber;

public class DefaultEventHandlerInvokerTests
{
    [Fact(DisplayName = "InvokeAsync_SingleInterfaceHandler_ShouldPassEventHeadersAndToken")]
    public async Task InvokeAsync_SingleInterfaceHandler_ShouldPassEventHeadersAndToken()
    {
        var probe = new InvocationProbe();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(probe);
        services.AddScoped<RecordingHandlerA>();
        services.AddEventBus(builder => builder.WithProfile<SingleHandlerProfile>());

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        var @event = new SampleEvent("single");
        var headers = new Headers { CorrelationId = "c-123" };
        using var cts = new CancellationTokenSource();

        await invoker.InvokeAsync(@event, headers, cts.Token);

        probe.Records.Should().ContainSingle();
        var record = probe.Records.First();
        record.HandlerName.Should().Be(nameof(RecordingHandlerA));
        record.Event.Should().Be(@event);
        record.Headers.Should().BeSameAs(headers);
        record.Token.Should().Be(cts.Token);
    }

    [Fact(DisplayName = "InvokeAsync_MultipleHandlers_ShouldExecuteAllWithDistinctScopedDependencies")]
    public async Task InvokeAsync_MultipleHandlers_ShouldExecuteAllWithDistinctScopedDependencies()
    {
        var probe = new InvocationProbe();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(probe);
        services.AddScoped<ScopedContext>();
        services.AddScoped<RecordingHandlerA>();
        services.AddScoped<RecordingHandlerB>();
        services.AddEventBus(builder => builder.WithProfile<MultiHandlerProfile>());

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        await invoker.InvokeAsync(new SampleEvent("multi"), new Headers());

        probe.Records.Should().HaveCount(2);
        probe.Records.Select(r => r.HandlerName).Should().BeEquivalentTo(new[]
        {
            nameof(RecordingHandlerA),
            nameof(RecordingHandlerB)
        });
        probe.Records.Select(r => r.ScopeId).Distinct().Should().HaveCount(2);
    }

    [Fact(DisplayName = "InvokeAsync_HandlerNotRegisteredInDI_ShouldThrowHandlerNotRegisteredException")]
    public async Task InvokeAsync_HandlerNotRegisteredInDI_ShouldThrowHandlerNotRegisteredException()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(builder => builder.WithProfile<UnregisteredHandlerProfile>());

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        var act = () => invoker.InvokeAsync(new SampleEvent("missing"), new Headers());

        var assertion = await act.Should().ThrowAsync<HandlerNotRegisteredException>();
        assertion.Which.EventType.Should().Be(typeof(SampleEvent));
        assertion.Which.HandlerType.Should().Be(typeof(UnregisteredHandler));
    }

    [Fact(DisplayName = "InvokeAsync_UnsubscribedEvent_ShouldCompleteWithoutThrowing")]
    public async Task InvokeAsync_UnsubscribedEvent_ShouldCompleteWithoutThrowing()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        var act = () => invoker.InvokeAsync(new UnsubscribedEvent(), new Headers());

        await act.Should().NotThrowAsync();
    }

    [Fact(DisplayName = "InvokeAsync_WithNullArguments_ShouldThrowArgumentNullException")]
    public async Task InvokeAsync_WithNullArguments_ShouldThrowArgumentNullException()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        var nullEventAct = () => invoker.InvokeAsync(null!, new Headers());
        var nullHeadersAct = () => invoker.InvokeAsync(new UnsubscribedEvent(), null!);

        await nullEventAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("@event");
        await nullHeadersAct.Should().ThrowAsync<ArgumentNullException>().WithParameterName("headers");
    }

    [Fact(DisplayName = "InvokeAsync_MultipleErrorHandlers_ShouldInvokeAllInOrder")]
    public async Task InvokeAsync_MultipleErrorHandlers_ShouldInvokeAllInOrder()
    {
        var errorOrder = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(errorOrder);
        services.AddScoped<AlwaysFailHandler>();
        services.AddScoped<FirstErrorHandler>();
        services.AddScoped<SecondErrorHandler>();
        services.AddEventBus(builder => builder.WithProfile<MultiErrorHandlerProfile>());

        await using var provider = services.BuildServiceProvider();
        var invoker = provider.GetRequiredService<IEventHandlerInvoker>();

        await invoker.InvokeAsync(new SampleEvent("boom"), new Headers());

        errorOrder.Should().Equal(nameof(FirstErrorHandler), nameof(SecondErrorHandler));
    }

    private sealed record SampleEvent(string Value);
    private sealed record UnsubscribedEvent;

    private sealed class ScopedContext
    {
        public Guid ScopeId { get; } = Guid.NewGuid();
    }

    private sealed record HandlerRecord(string HandlerName, SampleEvent Event, Headers Headers, CancellationToken Token, Guid ScopeId);

    private sealed class InvocationProbe
    {
        public ConcurrentBag<HandlerRecord> Records { get; } = new();
    }

    private sealed class RecordingHandlerA : IEventHandler<SampleEvent>
    {
        private readonly InvocationProbe _probe;
        private readonly ScopedContext? _scoped;

        public RecordingHandlerA(InvocationProbe probe, ScopedContext? scoped = null)
        {
            _probe = probe;
            _scoped = scoped;
        }

        public Task HandleAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Records.Add(new HandlerRecord(nameof(RecordingHandlerA), @event, headers, cancellationToken, _scoped?.ScopeId ?? Guid.Empty));
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingHandlerB : IEventHandler<SampleEvent>
    {
        private readonly InvocationProbe _probe;
        private readonly ScopedContext _scoped;

        public RecordingHandlerB(InvocationProbe probe, ScopedContext scoped)
        {
            _probe = probe;
            _scoped = scoped;
        }

        public Task HandleAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Records.Add(new HandlerRecord(nameof(RecordingHandlerB), @event, headers, cancellationToken, _scoped.ScopeId));
            return Task.CompletedTask;
        }
    }

    private sealed class UnregisteredHandler : IEventHandler<SampleEvent>
    {
        public Task HandleAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class AlwaysFailHandler : IEventHandler<SampleEvent>
    {
        public Task HandleAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken)
            => throw new InvalidOperationException("boom");
    }

    private sealed class FirstErrorHandler : IEventExceptionHandler
    {
        private readonly List<string> _order;
        public FirstErrorHandler(List<string> order) => _order = order;

        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken)
        {
            _order.Add(nameof(FirstErrorHandler));
            return Task.CompletedTask;
        }
    }

    private sealed class SecondErrorHandler : IEventExceptionHandler
    {
        private readonly List<string> _order;
        public SecondErrorHandler(List<string> order) => _order = order;

        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken)
        {
            _order.Add(nameof(SecondErrorHandler));
            context.Handled = true;
            return Task.CompletedTask;
        }
    }

    private sealed class SingleHandlerProfile : SubscriptionProfile
    {
        public SingleHandlerProfile() => this.WhenOccurs<SampleEvent>().ToDo<RecordingHandlerA>();
    }

    private sealed class MultiHandlerProfile : SubscriptionProfile
    {
        public MultiHandlerProfile() => this.WhenOccurs<SampleEvent>().ToDo<RecordingHandlerA>().ToDo<RecordingHandlerB>();
    }

    private sealed class UnregisteredHandlerProfile : SubscriptionProfile
    {
        public UnregisteredHandlerProfile() => this.WhenOccurs<SampleEvent>().ToDo<UnregisteredHandler>();
    }

    private sealed class MultiErrorHandlerProfile : SubscriptionProfile
    {
        public MultiErrorHandlerProfile() =>
            this.WhenOccurs<SampleEvent>()
                .ToDo<AlwaysFailHandler>()
                .CatchExceptionToDo<FirstErrorHandler>()
                .CatchExceptionToDo<SecondErrorHandler>();
    }
}
