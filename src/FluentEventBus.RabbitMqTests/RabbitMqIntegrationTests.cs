using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.Topology;
using FluentAssertions;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.Profile;
using FluentEventBus.RabbitMq;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqIntegrationTests
{
    [Fact(DisplayName = "RabbitMQ_DI_Publish_Consume_Smoke")]
    public async Task RabbitMq_DI_Publish_Consume_Smoke()
    {
        if (!await ShouldRunAsync())
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"feb.smoke.exchange.{suffix}";
        var queue = $"feb.smoke.queue.{suffix}";
        var probe = new SmokeProbe();

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<SmokeTestHandler>();
            },
            builder =>
            {
                builder.WithProfile<SmokeTestProfile>();
                builder.UseRabbitMqTransport(
                    ConfigureConnection,
                    bind =>
                    {
                        bind.GlobalExchange = exchange;
                        bind.GlobalQueue = queue;
                        bind.PrefetchCount = 5;
                    });
            });

        try
        {
            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new TestEvent("hello"), new Headers { CorrelationId = "smoke-corr" });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var (receivedEvent, receivedHeaders) = await probe.Completed.WaitAsync(cts.Token);

            receivedEvent.Message.Should().Be("hello");
            receivedHeaders.MessageId.Should().NotBeNullOrWhiteSpace();
            receivedHeaders.OccurredAt.Should().NotBeNull();
            receivedHeaders.CorrelationId.Should().Be("smoke-corr");

            await host.StopAsync();
        }
        finally
        {
            await CleanupTopologyAsync(host, new[] { queue }, new[] { exchange });
        }
    }

    [Fact(DisplayName = "RabbitMQ_Publish_Unroutable_ShouldThrowMessageReturnedException")]
    public async Task RabbitMq_Publish_Unroutable_ShouldThrowMessageReturnedException()
    {
        if (!await ShouldRunAsync())
        {
            return;
        }

        var exchange = $"feb.unroutable.exchange.{Guid.NewGuid():N}";

        using var host = BuildHost(
            _ => { },
            builder =>
            {
                builder.UseRabbitMqTransport(
                    ConfigureConnection,
                    bind => bind.GlobalExchange = exchange);
            });

        try
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();

            var act = () => publisher.PublishAsync(new UnroutableTestEvent("no-queue"));

            var assertion = await act.Should().ThrowAsync<RabbitMqMessageReturnedException>();
            assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.Unroutable);
            assertion.Which.ReturnReason.Should().Be("NO_ROUTE");
        }
        finally
        {
            await CleanupTopologyAsync(host, Array.Empty<string>(), new[] { exchange });
        }
    }

    [Fact(DisplayName = "RabbitMQ_PerEventBinding_ShouldRouteToDedicatedExchangeAndQueue")]
    public async Task RabbitMq_PerEventBinding_ShouldRouteToDedicatedExchangeAndQueue()
    {
        if (!await ShouldRunAsync())
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N");
        var globalExchange = $"feb.global.exchange.{suffix}";
        var globalQueue = $"feb.global.queue.{suffix}";
        var dedicatedExchange = $"feb.dedicated.exchange.{suffix}";
        var dedicatedQueue = $"feb.dedicated.queue.{suffix}";
        var probe = new DedicatedProbe();

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<DedicatedEventHandler>();
            },
            builder =>
            {
                builder.WithProfile<DedicatedProfile>();
                builder.UseRabbitMqTransport(
                    ConfigureConnection,
                    bind =>
                    {
                        bind.GlobalExchange = globalExchange;
                        bind.GlobalQueue = globalQueue;
                        bind.ForEvent<DedicatedTestEvent>()
                            .DeclareExchange(dedicatedExchange)
                            .DeclareQueue(dedicatedQueue);
                    });
            });

        try
        {
            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new DedicatedTestEvent("routed-exclusively"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var received = await probe.Completed.WaitAsync(cts.Token);
            received.Payload.Should().Be("routed-exclusively");

            var advancedBus = host.Services.GetRequiredService<RabbitMqConnectionProvider>().GetAdvancedBus();
            var stats = await advancedBus.GetQueueStatsAsync(dedicatedQueue, cts.Token);
            stats.Should().NotBeNull();

            await host.StopAsync();
        }
        finally
        {
            await CleanupTopologyAsync(host, new[] { dedicatedQueue, globalQueue }, new[] { dedicatedExchange, globalExchange });
        }
    }

    [Fact(DisplayName = "RabbitMQ_FailedHandler_ShouldRetryAndDeadLetterToDlq")]
    public async Task RabbitMq_FailedHandler_ShouldRetryAndDeadLetterToDlq()
    {
        if (!await ShouldRunAsync())
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"feb.dlq.exchange.{suffix}";
        var queue = $"feb.dlq.queue.{suffix}";
        var dlx = $"feb.dlq.dlx.{suffix}";
        var dlq = $"feb.dlq.dead.{suffix}";
        var probe = new RetryProbe();

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<AlwaysFailHandler>();
            },
            builder =>
            {
                builder.WithProfile<DeadLetterProfile>();
                builder.UseRabbitMqTransport(
                    ConfigureConnection,
                    bind =>
                    {
                        bind.GlobalExchange = exchange;
                        bind.GlobalQueue = queue;
                        bind.MaxRetryCount = 2;
                        bind.DeclareGlobalDeadLetter(dlx, dlq);
                    });
            });

        IDisposable? dlqConsumer = null;
        try
        {
            await host.StartAsync();

            var dlqMessageTcs = new TaskCompletionSource<(byte[] Body, int RetryCount)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var advancedBus = host.Services.GetRequiredService<RabbitMqConnectionProvider>().GetAdvancedBus();
            dlqConsumer = advancedBus.Consume(new Queue(dlq), (body, properties, _) =>
            {
                var retryCount = 0;
                if (properties?.Headers is not null &&
                    properties.Headers.TryGetValue(Headers.RetryCountKey, out var value))
                {
                    retryCount = value is byte[] bytes && int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed)
                        ? parsed
                        : Convert.ToInt32(value);
                }

                dlqMessageTcs.TrySetResult((body.ToArray(), retryCount));
                return Task.FromResult(AckStrategies.Ack);
            });

            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new FailingEvent("dead-letter-me"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var (dlqBody, dlqRetryCount) = await dlqMessageTcs.Task.WaitAsync(cts.Token);

            probe.Attempts.Should().Be(3);
            probe.ObservedRetryCounts.Should().Equal(0, 1, 2);
            dlqRetryCount.Should().Be(2);
            Encoding.UTF8.GetString(dlqBody).Should().Contain("dead-letter-me");

            await host.StopAsync();
        }
        finally
        {
            dlqConsumer?.Dispose();
            await CleanupTopologyAsync(host, new[] { queue, dlq }, new[] { exchange, dlx });
        }
    }

    [Fact(DisplayName = "RabbitMQ_MultipleEventsOnSharedQueue_ShouldDeliverAllEvents")]
    public async Task RabbitMq_MultipleEventsOnSharedQueue_ShouldDeliverAllEvents()
    {
        if (!await ShouldRunAsync())
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"feb.multi.exchange.{suffix}";
        var queue = $"feb.multi.queue.{suffix}";
        var probe = new MultiEventProbe();

        using var host = BuildHost(
            services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<MultiHandlerA>();
                services.AddScoped<MultiHandlerB>();
            },
            builder =>
            {
                builder.WithProfile<MultiEventProfile>();
                builder.UseRabbitMqTransport(
                    ConfigureConnection,
                    bind =>
                    {
                        bind.GlobalExchange = exchange;
                        bind.GlobalQueue = queue;
                    });
            });

        try
        {
            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new MultiEventA("event-a"));
            await publisher.PublishAsync(new MultiEventB("event-b"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await probe.AllReceived.WaitAsync(cts.Token);

            probe.ReceivedMessages.Should().BeEquivalentTo(new[] { "A:event-a", "B:event-b" });

            await host.StopAsync();
        }
        finally
        {
            await CleanupTopologyAsync(host, new[] { queue }, new[] { exchange });
        }
    }

    private static IHost BuildHost(Action<IServiceCollection> registerServices, Action<IEventBusBuilder> configureBus)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices(services =>
            {
                registerServices(services);
                services.AddEventBus(configureBus);
            })
            .Build();
    }

    private static void ConfigureConnection(RabbitMqConnectionOption opt)
    {
        var host = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
        var port = Environment.GetEnvironmentVariable("RABBITMQ_PORT");
        var targetHost = string.IsNullOrWhiteSpace(host) ? "localhost" : host;
        if (int.TryParse(port, out var parsedPort) && parsedPort != 5672)
        {
            targetHost = $"{targetHost}:{parsedPort}";
        }

        opt.Host = targetHost;
        opt.UserName = "guest";
        opt.Password = "guest";
    }

    private static async Task CleanupTopologyAsync(IHost host, IEnumerable<string> queues, IEnumerable<string> exchanges)
    {
        try
        {
            var advancedBus = host.Services.GetRequiredService<RabbitMqConnectionProvider>().GetAdvancedBus();
            foreach (var queue in queues)
            {
                try { await advancedBus.QueueDeleteAsync(queue); } catch { }
            }

            foreach (var exchange in exchanges)
            {
                try { await advancedBus.ExchangeDeleteAsync(new Exchange(exchange, ExchangeType.Topic)); } catch { }
            }
        }
        catch
        {
        }
    }

    private static async Task<bool> ShouldRunAsync()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable("RUN_RABBITMQ_TESTS"), "1", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var host = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
        if (string.IsNullOrWhiteSpace(host))
        {
            host = "localhost";
        }

        var portStr = Environment.GetEnvironmentVariable("RABBITMQ_PORT");
        var port = int.TryParse(portStr, out var parsedPort) ? parsedPort : 5672;

        return await CanConnectAsync(host, port);
    }

    private static async Task<bool> CanConnectAsync(string host, int port)
    {
        try
        {
            using var client = new TcpClient();
            var connectTask = client.ConnectAsync(host, port);
            var timeout = Task.Delay(TimeSpan.FromSeconds(2));
            var completed = await Task.WhenAny(connectTask, timeout);
            return completed == connectTask && client.Connected;
        }
        catch
        {
            return false;
        }
    }

    private sealed record TestEvent(string Message);
    private sealed record UnroutableTestEvent(string Message);
    private sealed record DedicatedTestEvent(string Payload);
    private sealed record FailingEvent(string Reason);
    private sealed record MultiEventA(string Value);
    private sealed record MultiEventB(string Value);

    private sealed class SmokeProbe
    {
        private readonly TaskCompletionSource<(TestEvent Event, Headers Headers)> _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<(TestEvent Event, Headers Headers)> Completed => _tcs.Task;
        public void Record(TestEvent @event, Headers headers) => _tcs.TrySetResult((@event, headers));
    }

    private sealed class DedicatedProbe
    {
        private readonly TaskCompletionSource<DedicatedTestEvent> _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<DedicatedTestEvent> Completed => _tcs.Task;
        public void Record(DedicatedTestEvent @event) => _tcs.TrySetResult(@event);
    }

    private sealed class RetryProbe
    {
        public int Attempts { get; private set; }
        public List<int> ObservedRetryCounts { get; } = new();

        public void RecordAttempt(Headers headers)
        {
            Attempts++;
            var retryCount = headers.TryGetValue(Headers.RetryCountKey, out var value) ? Convert.ToInt32(value) : 0;
            ObservedRetryCounts.Add(retryCount);
        }
    }

    private sealed class MultiEventProbe
    {
        private int _count;
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ConcurrentBag<string> ReceivedMessages { get; } = new();
        public Task AllReceived => _tcs.Task;

        public void Record(string item)
        {
            ReceivedMessages.Add(item);
            if (Interlocked.Increment(ref _count) == 2)
            {
                _tcs.TrySetResult();
            }
        }
    }

    private sealed class SmokeTestHandler : IEventHandler<TestEvent>
    {
        private readonly SmokeProbe _probe;
        public SmokeTestHandler(SmokeProbe probe) => _probe = probe;

        public Task HandleAsync(TestEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(@event, headers);
            return Task.CompletedTask;
        }
    }

    private sealed class DedicatedEventHandler : IEventHandler<DedicatedTestEvent>
    {
        private readonly DedicatedProbe _probe;
        public DedicatedEventHandler(DedicatedProbe probe) => _probe = probe;

        public Task HandleAsync(DedicatedTestEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record(@event);
            return Task.CompletedTask;
        }
    }

    private sealed class AlwaysFailHandler : IEventHandler<FailingEvent>
    {
        private readonly RetryProbe _probe;
        public AlwaysFailHandler(RetryProbe probe) => _probe = probe;

        public Task HandleAsync(FailingEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.RecordAttempt(headers);
            throw new InvalidOperationException("simulated handler failure");
        }
    }

    private sealed class MultiHandlerA : IEventHandler<MultiEventA>
    {
        private readonly MultiEventProbe _probe;
        public MultiHandlerA(MultiEventProbe probe) => _probe = probe;

        public Task HandleAsync(MultiEventA @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record($"A:{@event.Value}");
            return Task.CompletedTask;
        }
    }

    private sealed class MultiHandlerB : IEventHandler<MultiEventB>
    {
        private readonly MultiEventProbe _probe;
        public MultiHandlerB(MultiEventProbe probe) => _probe = probe;

        public Task HandleAsync(MultiEventB @event, Headers headers, CancellationToken cancellationToken)
        {
            _probe.Record($"B:{@event.Value}");
            return Task.CompletedTask;
        }
    }

    private sealed class SmokeTestProfile : SubscriptionProfile
    {
        public SmokeTestProfile() => this.WhenOccurs<TestEvent>().ToDo<SmokeTestHandler>();
    }

    private sealed class DedicatedProfile : SubscriptionProfile
    {
        public DedicatedProfile() => this.WhenOccurs<DedicatedTestEvent>().ToDo<DedicatedEventHandler>();
    }

    private sealed class DeadLetterProfile : SubscriptionProfile
    {
        public DeadLetterProfile() => this.WhenOccurs<FailingEvent>().ToDo<AlwaysFailHandler>();
    }

    private sealed class MultiEventProfile : SubscriptionProfile
    {
        public MultiEventProfile()
        {
            this.WhenOccurs<MultiEventA>().ToDo<MultiHandlerA>();
            this.WhenOccurs<MultiEventB>().ToDo<MultiHandlerB>();
        }
    }
}
