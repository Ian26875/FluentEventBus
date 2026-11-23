using FluentAssertions;
using FluentEventBus.Naming;

// Top-level global namespace type to test namespace fallback to "app".
public sealed class GlobalNamespacePinged { }

namespace FluentEventBus.Tests.Naming
{
    public class EventNameRegistryTests
    {
        [Fact(DisplayName = "Registries_OfDifferentInstances_AreFullyIsolated")]
        public void Registries_OfDifferentInstances_AreFullyIsolated()
        {
            var first = new EventNameRegistry();
            var second = new EventNameRegistry();

            first.Register(typeof(MapperCases.Ping));

            var resolved = first.GetEventType(first.GetEventName(typeof(MapperCases.Ping)));
            resolved.Should().Be(typeof(MapperCases.Ping));

            var act = () => second.GetEventType(first.GetEventName(typeof(MapperCases.Ping)));
            act.Should().Throw<KeyNotFoundException>();
        }

        [Fact(DisplayName = "Register_SameTypeTwice_ShouldBeIdempotent")]
        public void Register_SameTypeTwice_ShouldBeIdempotent()
        {
            var registry = new EventNameRegistry();

            registry.Register(typeof(OrderCreated));
            var act = () => registry.Register(typeof(OrderCreated));

            act.Should().NotThrow();
            registry.GetEventType(registry.GetEventName(typeof(OrderCreated))).Should().Be(typeof(OrderCreated));
        }

        [Fact(DisplayName = "Register_TwoDifferentTypesWithSameEventName_ShouldThrowArgumentException")]
        public void Register_TwoDifferentTypesWithSameEventName_ShouldThrowArgumentException()
        {
            var registry = new EventNameRegistry();
            registry.Register(typeof(DuplicateNameA));

            var act = () => registry.Register(typeof(DuplicateNameB));

            act.Should().Throw<ArgumentException>();
        }

        [Fact(DisplayName = "Register_NullType_ShouldThrowArgumentNullException")]
        public void Register_NullType_ShouldThrowArgumentNullException()
        {
            var registry = new EventNameRegistry();

            var act = () => registry.Register(null!);

            act.Should().Throw<ArgumentNullException>().WithParameterName("eventType");
        }

        [Fact(DisplayName = "GetEventType_UnregisteredName_ShouldThrowKeyNotFoundException")]
        public void GetEventType_UnregisteredName_ShouldThrowKeyNotFoundException()
        {
            var registry = new EventNameRegistry();

            var act = () => registry.GetEventType("unknown.event.happened.v1");

            act.Should().Throw<KeyNotFoundException>();
        }

        [Fact(DisplayName = "GetEventName_WithEventAttributeDefaultVersion_ShouldUseV1")]
        public void GetEventName_WithEventAttributeDefaultVersion_ShouldUseV1()
        {
            var registry = new EventNameRegistry();

            var name = registry.GetEventName(typeof(DefaultVersionEvent));

            name.Should().Be("shop.cart.checkedout.v1");
        }

        [Fact(DisplayName = "GetEventName_WithoutNamespace_ShouldFallbackDomainToApp")]
        public void GetEventName_WithoutNamespace_ShouldFallbackDomainToApp()
        {
            var registry = new EventNameRegistry();

            var name = registry.GetEventName(typeof(GlobalNamespacePinged));

            name.Should().Be("app.global.namespace.pinged.v1");
        }

        private sealed record OrderCreated(Guid Id);

        [Event("shared.conflict.event", 1)]
        private sealed record DuplicateNameA(Guid Id);

        [Event("shared.conflict.event", 1)]
        private sealed record DuplicateNameB(Guid Id);

        [Event("shop.cart.checkedout")]
        private sealed record DefaultVersionEvent(Guid Id);
    }
}
