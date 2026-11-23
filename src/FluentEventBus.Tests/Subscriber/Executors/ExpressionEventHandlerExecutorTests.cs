using System.Linq.Expressions;
using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.Subscriber;
using FluentEventBus.Subscriber.Executors;

namespace FluentEventBus.Tests.Subscriber.Executors;

public class ExpressionEventHandlerExecutorTests
{
    [Fact(DisplayName = "Constructor_ShouldInitializePropertiesCorrectly")]
    public void Constructor_ShouldInitializePropertiesCorrectly()
    {
        Expression<Func<TestHandler, Func<TestEvent, Headers, CancellationToken, Task>>> expression = h => h.Handle;

        var executor = new LambdaEventHandlerExecutor<TestEvent, TestHandler>(expression);

        executor.HandlerType.Should().Be(typeof(TestHandler));
        executor.EventType.Should().Be(typeof(TestEvent));
        executor.MethodInfo.Name.Should().Be("Handle");
    }

    [Fact(DisplayName = "CreateHandlerDelegate_ShouldReturnValidHandlerDelegate")]
    public async Task CreateHandlerDelegate_ShouldReturnValidHandlerDelegate()
    {
        Expression<Func<TestHandler, Func<TestEvent, Headers, CancellationToken, Task>>> expression = h => h.Handle;
        var executor = new LambdaEventHandlerExecutor<TestEvent, TestHandler>(expression);

        var testHandler = new TestHandler();
        var testEvent = new TestEvent(Guid.NewGuid(), "Test");
        var headers = new Headers();
        var cancellationToken = CancellationToken.None;

        var handlerDelegate = executor.CreateHandlerDelegate();

        await handlerDelegate(testHandler, testEvent, headers, cancellationToken);

        testHandler.HandledEvent.Should().Be(testEvent);
        testHandler.HandledHeaders.Should().BeEmpty();
    }

    [Fact(DisplayName = "CreateHandlerDelegate_WithoutHeaders_ShouldInvokeTwoParameterDelegate")]
    public async Task CreateHandlerDelegate_WithoutHeaders_ShouldInvokeTwoParameterDelegate()
    {
        Expression<Func<SimpleDelegateHandler, Func<TestEvent, CancellationToken, Task>>> expression = h => h.HandleWithoutHeadersAsync;
        var executor = new LambdaEventHandlerExecutor<TestEvent, SimpleDelegateHandler>(expression);

        var handler = new SimpleDelegateHandler();
        var testEvent = new TestEvent(Guid.NewGuid(), "NoHeaders");
        using var cts = new CancellationTokenSource();

        var handlerDelegate = executor.CreateHandlerDelegate();
        await handlerDelegate(handler, testEvent, new Headers { CorrelationId = "ignored" }, cts.Token);

        executor.MethodInfo.Name.Should().Be(nameof(SimpleDelegateHandler.HandleWithoutHeadersAsync));
        handler.HandledEvent.Should().Be(testEvent);
        handler.ReceivedToken.Should().Be(cts.Token);
    }

    [Fact(DisplayName = "Constructor_WithNonMethodExpression_ShouldThrowArgumentException")]
    public void Constructor_WithNonMethodExpression_ShouldThrowArgumentException()
    {
        Expression<Func<SimpleDelegateHandler, Func<TestEvent, CancellationToken, Task>>> invalidExpression =
            h => (e, ct) => Task.CompletedTask;

        var act = () => new LambdaEventHandlerExecutor<TestEvent, SimpleDelegateHandler>(invalidExpression);

        act.Should().Throw<ArgumentException>();
    }

    [Fact(DisplayName = "CreateHandlerDelegate_WhenHandlerTypeMismatch_ShouldThrowInvalidCastException")]
    public async Task CreateHandlerDelegate_WhenHandlerTypeMismatch_ShouldThrowInvalidCastException()
    {
        Expression<Func<TestHandler, Func<TestEvent, Headers, CancellationToken, Task>>> expression = h => h.Handle;
        var executor = new LambdaEventHandlerExecutor<TestEvent, TestHandler>(expression);

        var invalidHandler = new object();
        var testEvent = new TestEvent(Guid.NewGuid(), "Test");
        var headers = new Headers();
        var handlerDelegate = executor.CreateHandlerDelegate();

        var act = () => handlerDelegate(invalidHandler, testEvent, headers, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidCastException>();
    }

    [Fact(DisplayName = "CreateHandlerDelegate_WhenEventTypeMismatch_ShouldThrowInvalidCastException")]
    public async Task CreateHandlerDelegate_WhenEventTypeMismatch_ShouldThrowInvalidCastException()
    {
        Expression<Func<TestHandler, Func<TestEvent, Headers, CancellationToken, Task>>> expression = h => h.Handle;
        var executor = new LambdaEventHandlerExecutor<TestEvent, TestHandler>(expression);

        var testHandler = new TestHandler();
        var invalidEvent = new object();
        var headers = new Headers();
        var handlerDelegate = executor.CreateHandlerDelegate();

        var act = () => handlerDelegate(testHandler, invalidEvent, headers, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidCastException>();
    }

    [Fact(DisplayName = "InterfaceEventHandlerExecutor_ShouldInitializeAndInvokeHandleAsync")]
    public async Task InterfaceEventHandlerExecutor_ShouldInitializeAndInvokeHandleAsync()
    {
        var executor = new InterfaceEventHandlerExecutor<TestEvent, InterfaceTestHandler>();

        executor.EventType.Should().Be(typeof(TestEvent));
        executor.HandlerType.Should().Be(typeof(InterfaceTestHandler));
        executor.MethodInfo.Name.Should().Be(nameof(IEventHandler<TestEvent>.HandleAsync));

        var handler = new InterfaceTestHandler();
        var @event = new TestEvent(Guid.NewGuid(), "Interface");
        var headers = new Headers { CorrelationId = "c-1" };
        using var cts = new CancellationTokenSource();

        await executor.CreateHandlerDelegate()(handler, @event, headers, cts.Token);

        handler.HandledEvent.Should().Be(@event);
        handler.HandledHeaders.Should().BeSameAs(headers);
        handler.ReceivedToken.Should().Be(cts.Token);
    }

    private sealed class SimpleDelegateHandler
    {
        public TestEvent? HandledEvent { get; private set; }
        public CancellationToken ReceivedToken { get; private set; }

        public Task HandleWithoutHeadersAsync(TestEvent @event, CancellationToken cancellationToken)
        {
            HandledEvent = @event;
            ReceivedToken = cancellationToken;
            return Task.CompletedTask;
        }
    }

    private sealed class InterfaceTestHandler : IEventHandler<TestEvent>
    {
        public TestEvent? HandledEvent { get; private set; }
        public Headers? HandledHeaders { get; private set; }
        public CancellationToken ReceivedToken { get; private set; }

        public Task HandleAsync(TestEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            HandledEvent = @event;
            HandledHeaders = headers;
            ReceivedToken = cancellationToken;
            return Task.CompletedTask;
        }
    }
}
