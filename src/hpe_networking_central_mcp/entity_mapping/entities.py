"""Canonical domain entity definitions for Aruba Central.

Each Entity represents a real-world network concept (Device, Site, etc.)
with named fields that API parameters can map to.  EntityField captures
both the canonical field name and the property name used in the graph DB.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EntityField:
    """A single property on a domain entity.

    Attributes:
        name: Canonical field name (e.g. "serial", "mac").
        graph_property: Corresponding LadybugDB graph node property (e.g. "serial", "mac").
        description: Human-readable description of the field's semantics.
    """
    name: str
    graph_property: str
    description: str = ""


@dataclass(frozen=True)
class Entity:
    """A canonical domain entity.

    Attributes:
        name: Unique entity identifier (e.g. "Device", "Site").
        graph_node: The LadybugDB node table name (e.g. "Device", "Site").
        description: What this entity represents.
        fields: Named fields that API parameters can map to.
    """
    name: str
    graph_node: str
    description: str = ""
    fields: dict[str, EntityField] = field(default_factory=dict)

    def field(self, name: str) -> EntityField | None:
        return self.fields.get(name)


class EntityRegistry:
    """Registry of all canonical domain entities.

    Provides lookup by entity name and by (entity, field) pairs.
    """

    def __init__(self) -> None:
        self._entities: dict[str, Entity] = {}

    def register(self, entity: Entity) -> None:
        self._entities[entity.name] = entity

    def get(self, name: str) -> Entity | None:
        return self._entities.get(name)

    def all_entities(self) -> list[Entity]:
        return list(self._entities.values())

    def all_fields(self) -> list[tuple[Entity, EntityField]]:
        """Return all (entity, field) pairs."""
        result = []
        for entity in self._entities.values():
            for ef in entity.fields.values():
                result.append((entity, ef))
        return result

    def __len__(self) -> int:
        return len(self._entities)


def build_aruba_central_registry() -> EntityRegistry:
    """Build the canonical entity registry for Aruba Central.

    These entities correspond to the LadybugDB graph node tables defined in
    graph/schema.py, extended with the semantic meaning of each field.
    """
    registry = EntityRegistry()

    registry.register(Entity(
        name="Device",
        graph_node="Device",
        description="A managed network device (switch, AP, gateway)",
        fields={
            "serial": EntityField("serial", "serial", "Device serial number (primary key)"),
            "mac": EntityField("mac", "mac", "Device MAC address"),
            "name": EntityField("name", "name", "Device hostname / display name"),
            "model": EntityField("model", "model", "Hardware model identifier"),
            "device_type": EntityField("device_type", "deviceType", "Device type (AP, SWITCH, GATEWAY)"),
            "status": EntityField("status", "status", "Operational status (ONLINE/OFFLINE)"),
            "ipv4": EntityField("ipv4", "ipv4", "IPv4 management address"),
            "firmware": EntityField("firmware", "firmware", "Running firmware version"),
            "persona": EntityField("persona", "persona", "Device persona / role"),
            "device_function": EntityField("device_function", "deviceFunction", "Functional role (AP, SWITCH, GATEWAY, etc.)"),
            "id": EntityField("id", "id", "Device identifier (varies: serial, asset-tag, etc.)"),
        },
    ))

    registry.register(Entity(
        name="Site",
        graph_node="Site",
        description="A physical site / location containing devices",
        fields={
            "scope_id": EntityField("scope_id", "scopeId", "Site scope identifier (primary key)"),
            "name": EntityField("name", "name", "Site display name"),
            "address": EntityField("address", "address", "Street address"),
            "city": EntityField("city", "city", "City name"),
            "country": EntityField("country", "country", "Country code"),
        },
    ))

    registry.register(Entity(
        name="SiteCollection",
        graph_node="SiteCollection",
        description="A logical grouping of sites",
        fields={
            "scope_id": EntityField("scope_id", "scopeId", "Collection scope identifier (primary key)"),
            "name": EntityField("name", "name", "Collection display name"),
        },
    ))

    registry.register(Entity(
        name="DeviceGroup",
        graph_node="DeviceGroup",
        description="A cross-cutting device membership group",
        fields={
            "scope_id": EntityField("scope_id", "scopeId", "Group scope identifier (primary key)"),
            "name": EntityField("name", "name", "Group display name"),
        },
    ))

    registry.register(Entity(
        name="ConfigProfile",
        graph_node="ConfigProfile",
        description="A library-level configuration profile",
        fields={
            "id": EntityField("id", "id", "Profile unique identifier"),
            "name": EntityField("name", "name", "Profile display name"),
            "category": EntityField("category", "category", "Profile category (wlan-ssids, sw-port-profiles, etc.)"),
        },
    ))

    registry.register(Entity(
        name="Org",
        graph_node="Org",
        description="The organization root scope",
        fields={
            "scope_id": EntityField("scope_id", "scopeId", "Organization scope identifier"),
            "name": EntityField("name", "name", "Organization name"),
        },
    ))

    registry.register(Entity(
        name="UnmanagedDevice",
        graph_node="UnmanagedDevice",
        description="A third-party device discovered via LLDP",
        fields={
            "mac": EntityField("mac", "mac", "MAC address (primary key)"),
            "name": EntityField("name", "name", "Device name"),
        },
    ))

    # --- Non-graph entities (important API concepts not yet in the graph) ---

    registry.register(Entity(
        name="WLAN",
        graph_node="",  # not in graph yet
        description="A wireless LAN / SSID configuration",
        fields={
            "name": EntityField("name", "", "WLAN / SSID name"),
            "id": EntityField("id", "", "WLAN identifier"),
        },
    ))

    registry.register(Entity(
        name="VLAN",
        graph_node="",
        description="A VLAN configuration",
        fields={
            "id": EntityField("id", "", "VLAN numeric ID"),
            "name": EntityField("name", "", "VLAN name"),
        },
    ))

    registry.register(Entity(
        name="Tunnel",
        graph_node="",
        description="A VPN/overlay tunnel",
        fields={
            "name": EntityField("name", "", "Tunnel name"),
            "id": EntityField("id", "", "Tunnel identifier"),
        },
    ))

    registry.register(Entity(
        name="Port",
        graph_node="",
        description="A physical or logical port on a device",
        fields={
            "number": EntityField("number", "", "Port number"),
            "index": EntityField("index", "", "Port index"),
            "name": EntityField("name", "", "Port name/label"),
        },
    ))

    registry.register(Entity(
        name="Radio",
        graph_node="",
        description="A wireless radio on an access point",
        fields={
            "number": EntityField("number", "", "Radio number (0, 1, 2, ...)"),
        },
    ))

    registry.register(Entity(
        name="Floor",
        graph_node="",
        description="A floor/level within a site for location services",
        fields={
            "id": EntityField("id", "", "Floor identifier"),
        },
    ))

    registry.register(Entity(
        name="Report",
        graph_node="",
        description="A generated report",
        fields={
            "id": EntityField("id", "", "Report identifier"),
        },
    ))

    registry.register(Entity(
        name="Task",
        graph_node="",
        description="An async task / job in Central",
        fields={
            "id": EntityField("id", "", "Task / job identifier"),
        },
    ))

    registry.register(Entity(
        name="Cluster",
        graph_node="",
        description="A gateway cluster for HA",
        fields={
            "name": EntityField("name", "", "Cluster name"),
            "id": EntityField("id", "", "Cluster identifier"),
        },
    ))

    registry.register(Entity(
        name="Client",
        graph_node="",
        description="A network client connected to a device",
        fields={
            "mac": EntityField("mac", "", "Client MAC address"),
        },
    ))

    registry.register(Entity(
        name="Policy",
        graph_node="",
        description="A firewall / ACL / security policy",
        fields={
            "id": EntityField("id", "", "Policy identifier"),
            "name": EntityField("name", "", "Policy name"),
        },
    ))

    registry.register(Entity(
        name="Scope",
        graph_node="",
        description="Abstract scope in the config hierarchy (org, site, group, device)",
        fields={
            "id": EntityField("id", "", "Scope identifier"),
            "type": EntityField("type", "", "Scope type (org, site, site-collection, device-groups)"),
        },
    ))

    # --- GreenLake platform entities ---

    registry.register(Entity(
        name="Webhook",
        graph_node="",
        description="A GreenLake webhook subscription for event notifications",
        fields={
            "id": EntityField("id", "", "Webhook identifier"),
        },
    ))

    registry.register(Entity(
        name="Workspace",
        graph_node="",
        description="A GreenLake workspace / tenant context",
        fields={
            "id": EntityField("id", "", "Workspace identifier"),
        },
    ))

    registry.register(Entity(
        name="Credential",
        graph_node="",
        description="A GreenLake API credential / service account",
        fields={
            "id": EntityField("id", "", "Credential identifier"),
        },
    ))

    registry.register(Entity(
        name="Subscription",
        graph_node="",
        description="A GreenLake service provision / subscription",
        fields={
            "id": EntityField("id", "", "Subscription / service provision identifier"),
        },
    ))

    return registry
