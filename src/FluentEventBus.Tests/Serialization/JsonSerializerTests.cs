using FluentAssertions;
using FluentEventBus.Serialization;

namespace FluentEventBus.Tests.Serialization;

public class JsonSerializerTests
{
    [Fact(DisplayName = "SerializeAndDeserializeBytes_ShouldRoundTripPayload")]
    public void SerializeAndDeserializeBytes_ShouldRoundTripPayload()
    {
        var serializer = new JsonSerializer();
        var payload = new OrderPayload(Guid.NewGuid(), "Widget", 3);

        var bytes = serializer.Serialize(payload);
        var restored = serializer.Deserialize(bytes, typeof(OrderPayload)) as OrderPayload;

        restored.Should().Be(payload);
    }

    [Fact(DisplayName = "DeserializeString_ShouldBeCaseInsensitive")]
    public void DeserializeString_ShouldBeCaseInsensitive()
    {
        var serializer = new JsonSerializer();
        var id = Guid.NewGuid();
        var json = $"{{\"orderId\":\"{id}\",\"itemName\":\"gadget\",\"quantity\":7}}";

        var restored = serializer.Deserialize(json, typeof(OrderPayload)) as OrderPayload;

        restored.Should().Be(new OrderPayload(id, "gadget", 7));
    }

    private sealed record OrderPayload(Guid OrderId, string ItemName, int Quantity);
}
