"""Static rule mapper — exact name-to-entity mappings.

The highest-confidence mapper in the pipeline.  Maps API parameter names
and path variables to canonical entities via a hand-curated lookup table.
These rules are derived from the actual Aruba Central API analysis:

- 111 unique parameter names across 1476 endpoints
- Top params: scope-id (929x), device-function (924x), serial-number (258x), site-id (180x)
- Path variables: {name} (596x), {id} (34x)

The static rules handle the unambiguous cases — parameters whose meaning
is clear from the name alone, regardless of which endpoint uses them.
"""

from __future__ import annotations

from .entities import EntityRegistry
from .mapper import Confidence, Mapper, MappingResult, ParamContext

# ── Static lookup table ──────────────────────────────────────────────
#
# Format: param_name -> (entity_name, field_name, confidence, reason)
#
# Only includes mappings that are unambiguous from the parameter name alone.
# Ambiguous params like "name" or "id" are handled by context-aware mappers.

_STATIC_RULES: dict[str, tuple[str, str, Confidence, str]] = {
    # ── Device identifiers ───────────────────────────────────────
    "serial-number": ("Device", "serial", Confidence.EXACT,
                      "API serial-number always refers to a device serial"),
    "serial-numbers": ("Device", "serial", Confidence.EXACT,
                       "Plural form of device serial number"),
    "serial": ("Device", "serial", Confidence.EXACT,
               "Shorthand for device serial number"),
    "device_serial": ("Device", "serial", Confidence.EXACT,
                      "Explicit device serial parameter"),
    "conductor-serial": ("Device", "serial", Confidence.HIGH,
                         "Conductor device serial in cluster context"),
    "mac-address": ("Device", "mac", Confidence.HIGH,
                    "MAC address — usually refers to a device"),
    "macaddr": ("Device", "mac", Confidence.HIGH,
                "Alternate MAC address parameter name"),
    "device-type": ("Device", "device_type", Confidence.EXACT,
                    "Explicit device type filter"),

    # ── Site identifiers ─────────────────────────────────────────
    "site-id": ("Site", "scope_id", Confidence.EXACT,
                "Site scope identifier"),
    "site-ids": ("Site", "scope_id", Confidence.EXACT,
                 "Plural form — list of site scope identifiers"),
    "site-name": ("Site", "name", Confidence.EXACT,
                  "Explicit site name parameter"),
    "site-assigned": ("Site", "scope_id", Confidence.HIGH,
                      "Whether device is assigned to a site"),

    # ── Scope (abstract config hierarchy) ────────────────────────
    "scope-id": ("Scope", "id", Confidence.EXACT,
                 "Config hierarchy scope identifier (org/site/group/device)"),
    "scope-type": ("Scope", "type", Confidence.EXACT,
                   "Config hierarchy scope type"),

    # ── Device function (AP/SWITCH/GATEWAY — config routing) ─────
    "device-function": ("Device", "device_function", Confidence.EXACT,
                        "Device functional role used for config routing"),
    "object-type": ("ConfigProfile", "category", Confidence.HIGH,
                    "Config object type — routes to correct config category"),

    # ── Config view parameters ───────────────────────────────────
    "view-type": ("ConfigProfile", "category", Confidence.MEDIUM,
                  "Config view type (LIBRARY vs effective)"),
    "effective": ("ConfigProfile", "category", Confidence.MEDIUM,
                  "Whether to show effective (merged) configuration"),
    "detailed": ("ConfigProfile", "category", Confidence.MEDIUM,
                 "Whether to include detail annotations on config"),

    # ── Tunnel identifiers ───────────────────────────────────────
    "tunnel-name": ("Tunnel", "name", Confidence.EXACT,
                    "VPN/overlay tunnel name"),
    "tunnel-id": ("Tunnel", "id", Confidence.EXACT,
                  "VPN/overlay tunnel identifier"),

    # ── Port identifiers ─────────────────────────────────────────
    "port-number": ("Port", "number", Confidence.EXACT,
                    "Physical/logical port number"),
    "port-index": ("Port", "index", Confidence.EXACT,
                   "Port index identifier"),

    # ── Radio identifiers ────────────────────────────────────────
    "radio-number": ("Radio", "number", Confidence.EXACT,
                     "AP radio number (0, 1, 2, ...)"),

    # ── WLAN identifiers ─────────────────────────────────────────
    "wlan-name": ("WLAN", "name", Confidence.EXACT,
                  "Wireless LAN / SSID name"),

    # ── VLAN identifiers ─────────────────────────────────────────
    "vlan-id": ("VLAN", "id", Confidence.EXACT,
                "VLAN numeric identifier"),

    # ── Floor identifiers ────────────────────────────────────────
    "floor-id": ("Floor", "id", Confidence.EXACT,
                 "Floor plan identifier for location services"),

    # ── Report identifiers ───────────────────────────────────────
    "report-id": ("Report", "id", Confidence.EXACT,
                  "Report identifier"),

    # ── Task / Job identifiers ───────────────────────────────────
    "task-id": ("Task", "id", Confidence.EXACT,
                "Async task / job identifier"),
    "job-id": ("Task", "id", Confidence.EXACT,
               "Alias for task-id"),

    # ── Cluster identifiers ──────────────────────────────────────
    "cluster-name": ("Cluster", "name", Confidence.EXACT,
                     "Gateway cluster name"),

    # ── Client identifiers ───────────────────────────────────────
    "client-mac": ("Client", "mac", Confidence.EXACT,
                   "Network client MAC address"),
    "last-connect-client-mac-address": ("Client", "mac", Confidence.EXACT,
                                        "Last connected client MAC"),

    # ── Profile identifiers ──────────────────────────────────────
    "profile-name": ("ConfigProfile", "name", Confidence.HIGH,
                     "Configuration profile name"),
    "auth-profile-id": ("Policy", "id", Confidence.HIGH,
                        "Authentication profile identifier"),
    "policy-id": ("Policy", "id", Confidence.EXACT,
                  "Security/firewall policy identifier"),

    # ── GreenLake platform identifiers ───────────────────────────
    "cluster-id": ("Cluster", "id", Confidence.EXACT,
                   "Gateway cluster / swarm identifier"),
    "interface-id": ("Port", "number", Confidence.HIGH,
                     "Network interface identifier"),
    "report-run-id": ("Report", "id", Confidence.EXACT,
                      "Report run identifier"),
    "location-id": ("Site", "scope_id", Confidence.HIGH,
                    "Device location identifier"),
    "image-id": ("ConfigProfile", "id", Confidence.HIGH,
                 "CNAC image identifier"),
    "event-identifier": ("Webhook", "id", Confidence.MEDIUM,
                         "Event identifier in webhook context"),
    "identity-store-id": ("Policy", "id", Confidence.MEDIUM,
                          "Identity store / auth provider identifier"),

    # ── Pagination (not entity-related, mapped to a pseudo-entity) ───
    # These are intentionally NOT mapped — they're infrastructure params.
    # They will stay UNMAPPED, which is correct.
}


class StaticRuleMapper(Mapper):
    """Maps parameters using an exact name lookup table.

    This is the first mapper in the pipeline — it handles the easy cases
    with EXACT or HIGH confidence.
    """

    @property
    def name(self) -> str:
        return "static_rules"

    def map_param(self, ctx: ParamContext, registry: EntityRegistry) -> MappingResult:
        rule = _STATIC_RULES.get(ctx.param_name)
        if rule is None:
            return MappingResult(
                param_name=ctx.param_name,
                param_location=ctx.param_location,
                confidence=Confidence.UNMAPPED,
                mapper_name=self.name,
                endpoint_id=f"{ctx.endpoint_method}:{ctx.endpoint_path}",
            )

        entity_name, field_name, confidence, reason = rule

        # Validate the entity + field exist in the registry
        entity = registry.get(entity_name)
        if entity is None:
            return MappingResult(
                param_name=ctx.param_name,
                param_location=ctx.param_location,
                confidence=Confidence.UNMAPPED,
                mapper_name=self.name,
                reason=f"Static rule references unknown entity '{entity_name}'",
                endpoint_id=f"{ctx.endpoint_method}:{ctx.endpoint_path}",
            )

        return MappingResult(
            param_name=ctx.param_name,
            param_location=ctx.param_location,
            entity_name=entity_name,
            field_name=field_name,
            confidence=confidence,
            mapper_name=self.name,
            reason=reason,
            endpoint_id=f"{ctx.endpoint_method}:{ctx.endpoint_path}",
        )
