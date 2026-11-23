#if NET8_0_OR_GREATER
using System.Text.Json;
using FluentAssertions;
using FluentEventBus.AsyncApi;
using FluentEventBus.DependencyInjection;
using FluentEventBus.Event;
using FluentEventBus.Profile;
using FluentEventBus.Naming;
using FluentEventBus.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Neuroglia.AsyncApi;
using Neuroglia.AsyncApi.Generation;
using AsyncApiDocumentGenerator = FluentEventBus.AsyncApi.AsyncApiDocumentGenerator;

namespace FluentEventBus.Tests.AsyncApi;

public class AsyncApiDocumentGeneratorTests
{
    [Fact(DisplayName = "Generate_ShouldBuildChannelsAndReceiveOperationsFromProfiles")]
    public void Generate_ShouldBuildChannelsAndReceiveOperationsFromProfiles()
    {
        var generator = BuildGenerator();

        var document = generator.Generate();

        document.AsyncApi.Should().Be("3.0.0");
        document.Info.Title.Should().Be("Orders Service");
        document.Channels.Should().ContainKey("OrderShipped");
        document.Channels["OrderShipped"].Address.Should().Be("OrderShipped");
        document.Channels["OrderShipped"].Messages.Should().ContainKey(nameof(OrderShipped));
        document.Operations.Should().ContainKey("receive.OrderShipped");
        document.Operations["receive.OrderShipped"].Channel.Reference.Should().Be("#/channels/OrderShipped");
    }

    [Fact(DisplayName = "SerializeAsync_ShouldProduceJsonDocument")]
    public async Task SerializeAsync_ShouldProduceJsonDocument()
    {
        var generator = BuildGenerator();

        var json = await generator.SerializeAsync(AsyncApiDocumentFormat.Json);

        json.Should().Contain("\"asyncapi\"").And.Contain("3.0.0").And.Contain("OrderShipped");
    }

    [Fact(DisplayName = "Generate_WithServerDescriptor_ShouldDeclareServerFromTransport")]
    public void Generate_WithServerDescriptor_ShouldDeclareServerFromTransport()
    {
        var generator = BuildGenerator(services: services =>
            services.AddSingleton<IEventBusServerDescriptor>(
                new EventBusServerDescriptor("rabbitmq", "broker:5672", "amqp", "0.9.1")));

        var document = generator.Generate();

        document.Servers.Should().ContainKey("rabbitmq");
        document.Servers!["rabbitmq"].Host.Should().Be("broker:5672");
        document.Servers["rabbitmq"].Protocol.Should().Be("amqp");
    }

    [Fact(DisplayName = "Generate_ExplicitWithServer_ShouldOverrideDescriptor")]
    public void Generate_ExplicitWithServer_ShouldOverrideDescriptor()
    {
        var generator = BuildGenerator(
            options => options.WithServer("rabbitmq", "override:5672", "amqp"),
            services => services.AddSingleton<IEventBusServerDescriptor>(
                new EventBusServerDescriptor("rabbitmq", "broker:5672", "amqp")));

        var document = generator.Generate();

        document.Servers!["rabbitmq"].Host.Should().Be("override:5672");
    }

    [Fact(DisplayName = "Generate_WithExample_ShouldIncludeMessageExample")]
    public void Generate_WithExample_ShouldIncludeMessageExample()
    {
        var generator = BuildGenerator(options =>
            options.WithExample(new OrderShipped(Guid.NewGuid(), "DHL"), name: "sample", summary: "A shipped order."));

        var document = generator.Generate();

        var examples = document.Components!.Messages![nameof(OrderShipped)].Examples;
        examples.Should().NotBeNull().And.HaveCount(1);
        examples![0].Name.Should().Be("sample");
        examples[0].Payload.Should().ContainKey("Carrier");
    }

    [Fact(DisplayName = "Generate_ShouldProduceCanonicalComponentsWithRequiredAndStringFormats")]
    public async Task Generate_ShouldProduceCanonicalComponentsWithRequiredAndStringFormats()
    {
        var generator = BuildGenerator();

        var document = generator.Generate();
        document.Components.Should().NotBeNull();
        document.Components!.Schemas.Should().ContainKey("eventBusHeaders");
        document.Components.Schemas.Should().ContainKey(nameof(OrderShipped));
        document.Components.Messages.Should().ContainKey(nameof(OrderShipped));
        document.Components.Messages![nameof(OrderShipped)].Payload!.Reference.Should().Be($"#/components/schemas/{nameof(OrderShipped)}");
        document.Components.Messages[nameof(OrderShipped)].Headers!.Reference.Should().Be("#/components/schemas/eventBusHeaders");
        document.Channels["OrderShipped"].Messages[nameof(OrderShipped)].Reference.Should().Be($"#/components/messages/{nameof(OrderShipped)}");

        var json = await generator.SerializeAsync(AsyncApiDocumentFormat.Json);
        using var jsonDoc = JsonDocument.Parse(json);
        var schemaEntry = jsonDoc.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty(nameof(OrderShipped));
        var orderSchema = schemaEntry.TryGetProperty("schema", out var wrapped) ? wrapped : schemaEntry;

        var required = orderSchema.GetProperty("required").EnumerateArray().Select(e => e.GetString()).ToList();
        required.Should().Contain(new[] { "OrderId", "Carrier", "ShippedAt", "ProcessingTime" });
        required.Should().NotContain("Notes");

        var props = orderSchema.GetProperty("properties");
        props.GetProperty("ShippedAt").GetProperty("type").GetString().Should().Be("string");
        props.GetProperty("ShippedAt").GetProperty("format").GetString().Should().Be("date-time");
        props.GetProperty("ProcessingTime").GetProperty("type").GetString().Should().Be("string");
        props.GetProperty("ProcessingTime").GetProperty("format").GetString().Should().Be("duration");
    }

    [Fact(DisplayName = "Generate_WithZeroConfigOptions_ShouldPopulateDefaultInfo")]
    public void Generate_WithZeroConfigOptions_ShouldPopulateDefaultInfo()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(builder =>
        {
            builder.WithProfile<ShippingProfile>();
            builder.AddAsyncApiDocument();
        });
        services.AddSingleton<IEventNameRegistry>(new StubEventNameRegistry());

        using var provider = services.BuildServiceProvider();
        var generator = provider.GetRequiredService<FluentEventBus.AsyncApi.AsyncApiDocumentGenerator>();

        var document = generator.Generate();

        document.Info.Title.Should().NotBeNullOrWhiteSpace();
        document.Info.Version.Should().NotBeNullOrWhiteSpace();
    }

    [Fact(DisplayName = "DocumentProvider_ShouldServeGeneratedDocumentByTitleOrIdAndEnumeration")]
    public async Task DocumentProvider_ShouldServeGeneratedDocumentByTitleOrIdAndEnumeration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        BuildGeneratorCore(services, null);
        await using var provider = services.BuildServiceProvider();

        var docProvider = provider.GetRequiredService<IAsyncApiDocumentProvider>();

        var byTitle = await docProvider.GetDocumentAsync("Orders Service", "1.2.3");
        var byId = await docProvider.GetDocumentAsync("Orders Service");
        var enumerated = docProvider.ToList();

        byTitle.Should().NotBeNull();
        byTitle!.Title.Should().Be("Orders Service");
        byId.Should().BeSameAs(byTitle);
        enumerated.Should().ContainSingle().Which.Should().BeSameAs(byTitle);
    }

    private static AsyncApiDocumentGenerator BuildGenerator(Action<AsyncApiDocumentOptions>? extraSetup = null,
                                                            Action<IServiceCollection>? services = null)
    {
        var serviceCollection = new ServiceCollection();
        services?.Invoke(serviceCollection);
        BuildGeneratorCore(serviceCollection, extraSetup);
        return serviceCollection.BuildServiceProvider().GetRequiredService<AsyncApiDocumentGenerator>();
    }

    private static void BuildGeneratorCore(ServiceCollection services, Action<AsyncApiDocumentOptions>? extraSetup)
    {
        services.AddLogging();
        services.AddEventBus(builder =>
        {
            builder.WithProfile<ShippingProfile>();
            builder.AddAsyncApiDocument(options =>
            {
                options.Title = "Orders Service";
                options.Version = "1.2.3";
                extraSetup?.Invoke(options);
            });
        });
        services.AddSingleton<IEventNameRegistry>(new StubEventNameRegistry());
    }

    private sealed record OrderShipped(
        Guid OrderId,
        string Carrier,
        DateTimeOffset ShippedAt = default,
        TimeSpan ProcessingTime = default,
        string? Notes = null);

    private sealed class OrderShippedHandler : IEventHandler<OrderShipped>
    {
        public Task HandleAsync(OrderShipped @event, Headers headers, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ShippingProfile : SubscriptionProfile
    {
        public ShippingProfile()
        {
            this.WhenOccurs<OrderShipped>().ToDo<OrderShippedHandler>();
        }
    }

    private sealed class StubEventNameRegistry : IEventNameRegistry
    {
        private readonly Dictionary<string, Type> _types = new();

        public void Register(Type eventType) => _types[eventType.Name] = eventType;

        public string GetEventName(Type type) => type.Name;

        public Type GetEventType(string type) => _types[type];
    }
}
#endif
