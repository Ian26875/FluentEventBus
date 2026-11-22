using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using FluentEventBus;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.Profile;
using FluentEventBus.RabbitMq;
using FluentEventBus.Subscriber;

namespace FluentEventBus.RabbitMqTests;

public class RabbitMqIntegrationTests
{
    private static TaskCompletionSource<bool>? CurrentTcs;

    [Fact(DisplayName = "RabbitMQ_DI_Publish_Consume_Smoke")]
    public async Task RabbitMq_DI_Publish_Consume_Smoke()
    {
        if (!IsIntegrationEnabled() || !await CanConnectAsync("localhost", 5672))
        {
            return;
        }

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        CurrentTcs = tcs;

        try
        {
            using var host = Host.CreateDefaultBuilder()
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.SetMinimumLevel(LogLevel.Information);
                })
                .ConfigureServices(services =>
                {
                    services.AddEventBus(builder =>
                    {
                        builder.WithProfile<TestProfile>();
                        builder.ScanHandlersFrom(typeof(RabbitMqIntegrationTests).Assembly);
                        builder.UseRabbitMqTransport(
                            opt =>
                            {
                                opt.Host = "localhost";
                                opt.UserName = "guest";
                                opt.Password = "guest";
                            },
                            bind =>
                            {
                                bind.GlobalExchange = "eventbus.topic";
                                bind.GlobalQueue = "fluenteventbus.test";
                                bind.PrefetchCount = 5;
                            });
                    });
                })
                .Build();

            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new TestEvent("hello"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await tcs.Task.WaitAsync(cts.Token);

            await host.StopAsync();

            tcs.Task.IsCompletedSuccessfully.Should().BeTrue();
        }
        finally
        {
            CurrentTcs = null;
        }
    }

    [Fact(DisplayName = "RabbitMQ_Publish_Unroutable_ShouldThrowMessageReturnedException")]
    public async Task RabbitMq_Publish_Unroutable_ShouldThrowMessageReturnedException()
    {
        if (!IsIntegrationEnabled() || !await CanConnectAsync("localhost", 5672))
        {
            return;
        }

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddEventBus(builder =>
                {
                    builder.UseRabbitMqTransport(
                        opt =>
                        {
                            opt.Host = "localhost";
                            opt.UserName = "guest";
                            opt.Password = "guest";
                        },
                        bind =>
                        {
                            bind.GlobalExchange = $"eventbus.unroutable.{Guid.NewGuid():N}";
                        });
                });
            })
            .Build();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var act = () => publisher.PublishAsync(new UnroutableTestEvent("no-queue"));

        var assertion = await act.Should().ThrowAsync<RabbitMqMessageReturnedException>();
        assertion.Which.Reason.Should().Be(RabbitMqPublishFailureReason.Unroutable);
    }

    private static bool IsIntegrationEnabled()
    {
        return string.Equals(Environment.GetEnvironmentVariable("RUN_RABBITMQ_TESTS"), "1", StringComparison.OrdinalIgnoreCase);
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

    private sealed class TestHandler : IEventHandler<TestEvent>
    {
        public Task HandleAsync(TestEvent @event, Headers headers, CancellationToken cancellationToken)
        {
            CurrentTcs?.TrySetResult(true);
            return Task.CompletedTask;
        }
    }

    private sealed class TestProfile : SubscriptionProfile
    {
        public TestProfile()
        {
            this.WhenOccurs<TestEvent>().ToDo<TestHandler>();
        }
    }
}
