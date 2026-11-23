using System.Linq.Expressions;
using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.ExceptionHandlers;
using FluentEventBus.Profile;
using FluentEventBus.Subscriber;
using FluentEventBus.Subscriber.Executors;

namespace FluentEventBus.Tests.Profile;

public class SubscriptionProfileTests
{
    [Fact(DisplayName = "WhenOccurs_ToDoInterface_ShouldRegisterInterfaceExecutor")]
    public void WhenOccurs_ToDoInterface_ShouldRegisterInterfaceExecutor()
    {
        var profile = new InterfaceProfile();

        profile.EventHandlerExecutors.Should().ContainKey(typeof(SampleEvent));
        var executor = profile.EventHandlerExecutors[typeof(SampleEvent)].Should().ContainSingle().Subject;
        executor.Should().BeOfType<InterfaceEventHandlerExecutor<SampleEvent, InterfaceHandler>>();
        executor.HandlerType.Should().Be(typeof(InterfaceHandler));
    }

    [Fact(DisplayName = "WhenOccurs_ToDoDelegateWithAndWithoutHeaders_ShouldRegisterLambdaExecutors")]
    public void WhenOccurs_ToDoDelegateWithAndWithoutHeaders_ShouldRegisterLambdaExecutors()
    {
        var profile = new DelegateProfile();

        var executors = profile.EventHandlerExecutors[typeof(SampleEvent)];
        executors.Should().HaveCount(2);
        executors.Should().AllBeOfType<LambdaEventHandlerExecutor<SampleEvent, DelegateService>>();
        executors.Select(e => e.MethodInfo.Name).Should().BeEquivalentTo(new[]
        {
            nameof(DelegateService.HandleWithHeadersAsync),
            nameof(DelegateService.HandleWithoutHeadersAsync)
        });
    }

    [Fact(DisplayName = "ToDo_WithNullExpression_ShouldThrowArgumentNullException")]
    public void ToDo_WithNullExpression_ShouldThrowArgumentNullException()
    {
        var act = () => new NullExpressionProfile();

        act.Should().Throw<ArgumentNullException>().WithParameterName("expression");
    }

    [Fact(DisplayName = "CatchExceptionToDo_ShouldRegisterErrorHandlerAndRejectDuplicates")]
    public void CatchExceptionToDo_ShouldRegisterErrorHandlerAndRejectDuplicates()
    {
        var profile = new ErrorHandlerProfile();

        profile.ErrorHandlers[typeof(SampleEvent)].Should().ContainSingle().Which.Should().Be(typeof(SampleErrorHandler));

        var duplicateAct = () => new DuplicateErrorHandlerProfile();
        duplicateAct.Should().Throw<ArgumentException>();
    }

    private sealed record SampleEvent(Guid Id);

    private sealed class InterfaceHandler : IEventHandler<SampleEvent>
    {
        public Task HandleAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class DelegateService
    {
        public Task HandleWithHeadersAsync(SampleEvent @event, Headers headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task HandleWithoutHeadersAsync(SampleEvent @event, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class SampleErrorHandler : IEventExceptionHandler
    {
        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class InterfaceProfile : SubscriptionProfile
    {
        public InterfaceProfile()
        {
            this.WhenOccurs<SampleEvent>().ToDo<InterfaceHandler>();
        }
    }

    private sealed class DelegateProfile : SubscriptionProfile
    {
        public DelegateProfile()
        {
            this.WhenOccurs<SampleEvent>()
                .ToDo<DelegateService>(s => s.HandleWithHeadersAsync)
                .ToDo<DelegateService>(s => s.HandleWithoutHeadersAsync);
        }
    }

    private sealed class NullExpressionProfile : SubscriptionProfile
    {
        public NullExpressionProfile()
        {
            Expression<Func<DelegateService, Func<SampleEvent, Headers, CancellationToken, Task>>>? expr = null;
            this.WhenOccurs<SampleEvent>().ToDo(expr!);
        }
    }

    private sealed class ErrorHandlerProfile : SubscriptionProfile
    {
        public ErrorHandlerProfile()
        {
            this.WhenOccurs<SampleEvent>()
                .ToDo<InterfaceHandler>()
                .CatchExceptionToDo<SampleErrorHandler>();
        }
    }

    private sealed class DuplicateErrorHandlerProfile : SubscriptionProfile
    {
        public DuplicateErrorHandlerProfile()
        {
            this.WhenOccurs<SampleEvent>()
                .ToDo<InterfaceHandler>()
                .CatchExceptionToDo<SampleErrorHandler>()
                .CatchExceptionToDo<SampleErrorHandler>();
        }
    }
}
