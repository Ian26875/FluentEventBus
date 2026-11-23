using FluentAssertions;
using FluentEventBus.Event;
using FluentEventBus.InMemory;
using Microsoft.Extensions.Logging.Abstractions;

namespace FluentEventBus.Tests.InMemory;

public class BackgroundQueueTests
{
    [Fact(DisplayName = "EnqueueAndDequeue_ShouldPreserveFifoOrderAndTrackPendingCount")]
    public async Task EnqueueAndDequeue_ShouldPreserveFifoOrderAndTrackPendingCount()
    {
        var queue = new BackgroundQueue(
            NullLogger<BackgroundQueue>.Instance,
            new BackgroundQueueOptions { Capacity = 10, AlertThreshold = 100 });

        var first = new EventContext(new byte[] { 1 }, new Headers { MessageId = "m1" }, "event.one.v1");
        var second = new EventContext(new byte[] { 2 }, new Headers { MessageId = "m2" }, "event.two.v1");

        await queue.EnqueueAsync(first);
        await queue.EnqueueAsync(second);

        queue.PendingCount.Should().Be(2);

        var dequeued1 = await queue.DequeueAsync();
        queue.PendingCount.Should().Be(1);

        var dequeued2 = await queue.DequeueAsync();
        queue.PendingCount.Should().Be(0);

        dequeued1.Should().BeSameAs(first);
        dequeued2.Should().BeSameAs(second);
    }

    [Fact(DisplayName = "EnqueueAsync_WhenPendingReachesAlertThreshold_ShouldInvokeOnAlert")]
    public async Task EnqueueAsync_WhenPendingReachesAlertThreshold_ShouldInvokeOnAlert()
    {
        var alertCalls = 0;
        var queue = new BackgroundQueue(
            NullLogger<BackgroundQueue>.Instance,
            new BackgroundQueueOptions
            {
                Capacity = 10,
                AlertThreshold = 2,
                OnAlert = () =>
                {
                    Interlocked.Increment(ref alertCalls);
                    return Task.CompletedTask;
                }
            });

        await queue.EnqueueAsync(new EventContext(ReadOnlyMemory<byte>.Empty, new Headers(), "e1"));
        alertCalls.Should().Be(0);

        await queue.EnqueueAsync(new EventContext(ReadOnlyMemory<byte>.Empty, new Headers(), "e2"));
        alertCalls.Should().Be(1);
    }

    [Fact(DisplayName = "EnqueueAsync_WhenQueueFull_ShouldWaitUntilDequeued")]
    public async Task EnqueueAsync_WhenQueueFull_ShouldWaitUntilDequeued()
    {
        var queue = new BackgroundQueue(
            NullLogger<BackgroundQueue>.Instance,
            new BackgroundQueueOptions { Capacity = 1, AlertThreshold = 10 });

        var item1 = new EventContext(new byte[] { 1 }, new Headers(), "e1");
        var item2 = new EventContext(new byte[] { 2 }, new Headers(), "e2");

        await queue.EnqueueAsync(item1);

        var secondEnqueueTask = queue.EnqueueAsync(item2).AsTask();
        await Task.Delay(50);
        secondEnqueueTask.IsCompleted.Should().BeFalse();

        var firstOut = await queue.DequeueAsync();
        firstOut.Should().BeSameAs(item1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await secondEnqueueTask.WaitAsync(cts.Token);

        var secondOut = await queue.DequeueAsync(cts.Token);
        secondOut.Should().BeSameAs(item2);
    }
}
