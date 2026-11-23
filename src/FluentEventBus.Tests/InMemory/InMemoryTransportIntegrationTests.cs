using System.Collections.Concurrent;
using System.Diagnostics;
using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.ExceptionHandlers;
using FluentEventBus.Metrics;
using FluentEventBus.Profile;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FluentEventBus.Tests.InMemory;

public class InMemoryTransportIntegrationTests
{
    [Fact(DisplayName = "InMemory_EndToEnd_ShouldDeliverToInterfaceAndDelegateHandlers")]
    public async Task InMemory_EndToEnd_ShouldDeliverToInterfaceAndDelegateHandlers()
    {
        var probe = new DeliveryProbe(expectedCount: 3);
        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<InterfaceOrderHandler>();
                services.AddScoped<DelegateWithHeadersService>();
                services.AddScoped<DelegateWithoutHeadersService>();
            },
            builder =>
            {
                builder.WithProfile<AllHandlersProfile>();
                builder.UseInMemoryTransport();
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            var id = Guid.NewGuid();
            await publisher.PublishAsync(new OrderCreated(id, "Widget"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await probe.AllReceived.WaitAsync(cts.Token);

            probe.HandlerCalls.Should().BeEquivalentTo(new[]
            {
                nameof(InterfaceOrderHandler),
                nameof(DelegateWithHeadersService.WithHeadersAsync),
                nameof(DelegateWithoutHeadersService.WithoutHeadersAsync)
            });
            probe.CapturedEvents.Should().AllBeEquivalentTo(new OrderCreated(id, "Widget"));
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact(DisplayName = "InMemory_EndToEnd_ShouldPropagateEnvelopeHeadersAndTraceParent")]
    public async Task InMemory_EndToEnd_ShouldPropagateEnvelopeHeadersAndTraceParent()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == EventBusTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        var probe = new HeaderProbe();
        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<HeaderCapturingHandler>();
            },
            builder =>
            {
                builder.WithProfile<HeaderProfile>();
                builder.UseInMemoryTransport();
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(
                new OrderCreated(Guid.NewGuid(), "Traced"),
                new Headers { CorrelationId = "trace-corr-42" });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var headers = await probe.Completed.WaitAsync(cts.Token);

            headers.MessageId.Should().NotBeNullOrWhiteSpace();
            headers.OccurredAt.Should().NotBeNull();
            headers.CorrelationId.Should().Be("trace-corr-42");
            headers.Should().ContainKey("traceparent");
            ActivityContext.TryParse(headers["traceparent"].ToString(), null, out var parsedTrace).Should().BeTrue();
            probe.ConsumeTraceId.Should().Be(parsedTrace.TraceId.ToString());
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact(DisplayName = "InMemory_MultipleHandlers_ShouldReceiveDistinctScopedInstances")]
    public async Task InMemory_MultipleHandlers_ShouldReceiveDistinctScopedInstances()
    {
        var probe = new ScopeProbe(expectedCount: 2);
        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<ScopedUnitOfWork>();
                services.AddScoped<ScopedHandlerOne>();
                services.AddScoped<ScopedHandlerTwo>();
            },
            builder =>
            {
                builder.WithProfile<ScopedProfile>();
                builder.UseInMemoryTransport();
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new OrderCreated(Guid.NewGuid(), "Scoped"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await probe.AllReceived.WaitAsync(cts.Token);

            probe.ScopeIds.Should().HaveCount(2);
            probe.ScopeIds.Distinct().Should().HaveCount(2);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact(DisplayName = "InMemory_AlwaysFailingHandler_ShouldRetryMaxTimesAndInvokePoisonCallback")]
    public async Task InMemory_AlwaysFailingHandler_ShouldRetryMaxTimesAndInvokePoisonCallback()
    {
        var retryProbe = new RetryProbe();
        var poisonTcs = new TaskCompletionSource<(EventContext Context, Exception Exception)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(retryProbe);
                services.AddScoped<AlwaysFailRetryHandler>();
            },
            builder =>
            {
                builder.WithProfile<AlwaysFailProfile>();
                builder.UseInMemoryTransport(
                    maxRetryCount: 2,
                    onPoisonMessage: (context, exception) =>
                    {
                        poisonTcs.TrySetResult((context, exception));
                        return Task.CompletedTask;
                    });
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new OrderCreated(Guid.NewGuid(), "Poison"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var (poisonContext, poisonException) = await poisonTcs.Task.WaitAsync(cts.Token);

            retryProbe.Attempts.Should().Be(3);
            retryProbe.ObservedRetryCounts.Should().Equal(0, 1, 2);
            poisonException.Should().BeOfType<InvalidOperationException>().Which.Message.Should().Be("permanent failure");
            poisonContext.Headers[Headers.RetryCountKey].Should().Be(2);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact(DisplayName = "InMemory_TransientFailure_ShouldSucceedOnRetryWithoutPoisonCallback")]
    public async Task InMemory_TransientFailure_ShouldSucceedOnRetryWithoutPoisonCallback()
    {
        var retryProbe = new RetryProbe { FailBeforeSuccessCount = 1 };
        var poisonCalled = false;

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(retryProbe);
                services.AddScoped<TransientFailHandler>();
            },
            builder =>
            {
                builder.WithProfile<TransientFailProfile>();
                builder.UseInMemoryTransport(
                    maxRetryCount: 3,
                    onPoisonMessage: (_, _) =>
                    {
                        poisonCalled = true;
                        return Task.CompletedTask;
                    });
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new OrderCreated(Guid.NewGuid(), "Transient"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await retryProbe.Succeeded.WaitAsync(cts.Token);

            // Give the subscriber loop a brief turn to verify no poison callback fires afterward.
            await Task.Delay(50, cts.Token);

            retryProbe.Attempts.Should().Be(2);
            retryProbe.ObservedRetryCounts.Should().Equal(0, 1);
            poisonCalled.Should().BeFalse();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact(DisplayName = "InMemory_HandledByExceptionHandler_ShouldSwallowWithoutRetryOrPoison")]
    public async Task InMemory_HandledByExceptionHandler_ShouldSwallowWithoutRetryOrPoison()
    {
        var retryProbe = new RetryProbe();
        var errorHandledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var poisonCalled = false;

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(retryProbe);
                services.AddSingleton(errorHandledTcs);
                services.AddScoped<AlwaysFailRetryHandler>();
                services.AddScoped<SwallowingExceptionHandler>();
            },
            builder =>
            {
                builder.WithProfile<SwallowedFailureProfile>();
                builder.UseInMemoryTransport(
                    maxRetryCount: 3,
                    onPoisonMessage: (_, _) =>
                    {
                        poisonCalled = true;
                        return Task.CompletedTask;
                    });
            });

        await host.StartAsync();
        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new OrderCreated(Guid.NewGuid(), "Swallowed"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await errorHandledTcs.Task.WaitAsync(cts.Token);

            // Allow time to confirm no retry or poison invocation is scheduled.
            await Task.Delay(50, cts.Token);

            retryProbe.Attempts.Should().Be(1);
            poisonCalled.Should().BeFalse();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static IHost BuildHost(Action<IServiceCollection> registerServices, Action<IEventBusBuilder> configureBus)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices(services =>
            {
                registerServices(services);
                services.AddEventBus(configureBus);
            })
            .Build();
    }

    private sealed record OrderCreated(Guid OrderId, string Item);

    private sealed class DeliveryProbe
    {
        private readonly int _expectedCount;
        private int _receivedCount;
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public DeliveryProbe(int expectedCount) => _expectedCount = expectedCount;

        public ConcurrentBag<string> HandlerCalls { get; } = new();
        public ConcurrentBag<OrderCreated> CapturedEvents { get; } = new();
        public Task AllReceived => _tcs.Task;

        public void Record(string handlerName, OrderCreated @event)
        {
            HandlerCalls.Add(handlerName);
            CapturedEvents.Add(@event);
            if (Interlocked.Increment(ref _receivedCount) == _expectedCount)
            {
                _tcs.TrySetResult();
            }
        }
    }

    private sealed class HeaderProbe
    {
        private readonly TaskCompletionSource<Headers> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public string? ConsumeTraceId { get; private set; }
        public Task<Headers> Completed => _tcs.Task;

        public void Record(Headers headers, string? consumeTraceId)
        {
            ConsumeTraceId = consumeTraceId;
            _tcs.TrySetResult(headers);
        }
    }

    private sealed class ScopeProbe
    {
        private readonly int _expectedCount;
        private int _count;
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ScopeProbe(int expectedCount) => _expectedCount = expectedCount;

        public ConcurrentBag<Guid> ScopeIds { get; } = new();
        public Task AllReceived => _tcs.Task;

        public void Record(Guid scopeId)
        {
            ScopeIds.Add(scopeId);
            if (Interlocked.Increment(ref _count) == _expectedCount)
            {
                _tcs.TrySetResult();
            }
        }
    }

    private sealed class RetryProbe
    {
        private readonly TaskCompletionSource _succeededTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int FailBeforeSuccessCount { get; init; }
        public int Attempts { get; private set; }
        public List<int> ObservedRetryCounts { get; } = new();
        public Task Succeeded => _succeededTcs.Task;

        public int RecordAttempt(Headers headers)
        {
            var attempt = ++Attempts;
            var retryCount = headers.TryGetValue(Headers.RetryCountKey, out var value) ? Convert.ToInt32(value) : 0;
            ObservedRetryCounts.Add(retryCount);
            return attempt;
        }

        public void MarkSucceeded() => _succeededTcs.TrySetResult();
    }

    private sealed class ScopedUnitOfWork
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    private sealed class InterfaceOrderHandler : IEventHandler<OrderCreated>
    {
        private readonly DeliveryProbe _probe;
        public InterfaceOrderHandler(DeliveryProbe probe) => _probe = probe;

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(nameof(InterfaceOrderHandler), @event);
            return Task.CompletedTask;
        }
    }

    private sealed class DelegateWithHeadersService
    {
        private readonly DeliveryProbe _probe;
        public DelegateWithHeadersService(DeliveryProbe probe) => _probe = probe;

        public Task WithHeadersAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(nameof(WithHeadersAsync), @event);
            return Task.CompletedTask;
        }
    }

    private sealed class DelegateWithoutHeadersService
    {
        private readonly DeliveryProbe _probe;
        public DelegateWithoutHeadersService(DeliveryProbe probe) => _probe = probe;

        public Task WithoutHeadersAsync(OrderCreated @event, CancellationToken cancellationToken)
        {
            _probe.Record(nameof(WithoutHeadersAsync), @event);
            return Task.CompletedTask;
        }
    }

    private sealed class HeaderCapturingHandler : IEventHandler<OrderCreated>
    {
        private readonly HeaderProbe _probe;
        public HeaderCapturingHandler(HeaderProbe probe) => _probe = probe;

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(headers, Activity.Current?.TraceId.ToString());
            return Task.CompletedTask;
        }
    }

    private sealed class ScopedHandlerOne : IEventHandler<OrderCreated>
    {
        private readonly ScopeProbe _probe;
        private readonly ScopedUnitOfWork _uow;

        public ScopedHandlerOne(ScopeProbe probe, ScopedUnitOfWork uow)
        {
            _probe = probe;
            _uow = uow;
        }

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(_uow.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class ScopedHandlerTwo : IEventHandler<OrderCreated>
    {
        private readonly ScopeProbe _probe;
        private readonly ScopedUnitOfWork _uow;

        public ScopedHandlerTwo(ScopeProbe probe, ScopedUnitOfWork uow)
        {
            _probe = probe;
            _uow = uow;
        }

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(_uow.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class AlwaysFailRetryHandler : IEventHandler<OrderCreated>
    {
        private readonly RetryProbe _probe;
        public AlwaysFailRetryHandler(RetryProbe probe) => _probe = probe;

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.RecordAttempt(headers);
            throw new InvalidOperationException("permanent failure");
        }
    }

    private sealed class TransientFailHandler : IEventHandler<OrderCreated>
    {
        private readonly RetryProbe _probe;
        public TransientFailHandler(RetryProbe probe) => _probe = probe;

        public Task HandleAsync(OrderCreated @event, Headers headers, CancellationToken cancellationToken)
        {
            var attempt = _probe.RecordAttempt(headers);
            if (attempt <= _probe.FailBeforeSuccessCount)
            {
                throw new InvalidOperationException("transient failure");
            }

            _probe.MarkSucceeded();
            return Task.CompletedTask;
        }
    }

    private sealed class SwallowingExceptionHandler : IEventExceptionHandler
    {
        private readonly TaskCompletionSource<bool> _tcs;
        public SwallowingExceptionHandler(TaskCompletionSource<bool> tcs) => _tcs = tcs;

        public Task OnExceptionAsync(ExceptionContext context, CancellationToken cancellationToken)
        {
            context.Handled = true;
            _tcs.TrySetResult(true);
            return Task.CompletedTask;
        }
    }

    private sealed class AllHandlersProfile : SubscriptionProfile
    {
        public AllHandlersProfile() =>
            this.WhenOccurs<OrderCreated>()
                .ToDo<InterfaceOrderHandler>()
                .ToDo<DelegateWithHeadersService>(s => s.WithHeadersAsync)
                .ToDo<DelegateWithoutHeadersService>(s => s.WithoutHeadersAsync);
    }

    private sealed class HeaderProfile : SubscriptionProfile
    {
        public HeaderProfile() => this.WhenOccurs<OrderCreated>().ToDo<HeaderCapturingHandler>();
    }

    private sealed class ScopedProfile : SubscriptionProfile
    {
        public ScopedProfile() => this.WhenOccurs<OrderCreated>().ToDo<ScopedHandlerOne>().ToDo<ScopedHandlerTwo>();
    }

    private sealed class AlwaysFailProfile : SubscriptionProfile
    {
        public AlwaysFailProfile() => this.WhenOccurs<OrderCreated>().ToDo<AlwaysFailRetryHandler>();
    }

    private sealed class TransientFailProfile : SubscriptionProfile
    {
        public TransientFailProfile() => this.WhenOccurs<OrderCreated>().ToDo<TransientFailHandler>();
    }

    private sealed class SwallowedFailureProfile : SubscriptionProfile
    {
        public SwallowedFailureProfile() =>
            this.WhenOccurs<OrderCreated>()
                .ToDo<AlwaysFailRetryHandler>()
                .CatchExceptionToDo<SwallowingExceptionHandler>();
    }
}
