"""Pattern-based mapper — resolves ambiguous params using endpoint context.

Handles cases where the parameter name alone is ambiguous (e.g. {name},
{id}) but the endpoint path or category provides disambiguation.

For example:
  - GET /network-config/v1/sites/{name} → Site.name
  - GET /network-config/v1/tunnels/{name} → Tunnel.name
  - GET /monitoring/v1/devices/{serial-number}/ports/{port-number} → Port.number
  - GET /network-config/v1alpha1/bgp/{name} → ConfigProfile.name (config resource)

This mapper uses regex patterns against the endpoint path to determine
which entity a generic parameter refers to.
"""

from __future__ import annotations

import re

from .entities import EntityRegistry
from .mapper import Confidence, Mapper, MappingResult, ParamContext

# ── Path-based disambiguation rules ─────────────────────────────────
#
# Format: (path_regex, param_name, entity_name, field_name, confidence, reason)
#
# Rules are evaluated in order; first match wins.
# More specific rules MUST come before more general ones.

_PATH_RULES: list[tuple[str, str, str, str, Confidence, str]] = [
    # ── Site-related paths ───────────────────────────────────────
    (r"/sites?/\{name\}", "name", "Site", "name", Confidence.EXACT,
     "Path /sites/{name} — name refers to a site"),
    (r"/sites?/\{id\}", "id", "Site", "scope_id", Confidence.EXACT,
     "Path /sites/{id} — id refers to a site scope"),
    (r"/site-collections?/\{name\}", "name", "SiteCollection", "name", Confidence.EXACT,
     "Path /site-collections/{name} — refers to a site collection"),

    # ── Device-group paths ───────────────────────────────────────
    (r"/device-groups?/\{name\}", "name", "DeviceGroup", "name", Confidence.EXACT,
     "Path /device-groups/{name} — refers to a device group"),

    # ── WLAN/SSID paths ─────────────────────────────────────────
    (r"/(?:wlan-ssids?|ssids?)/\{name\}", "name", "WLAN", "name", Confidence.EXACT,
     "Path /wlan-ssids/{name} — refers to a WLAN/SSID"),
    (r"/(?:wlan-ssids?|ssids?)/\{ssid\}", "ssid", "WLAN", "name", Confidence.EXACT,
     "Path /ssids/{ssid} — refers to a WLAN/SSID name"),
    (r"/overlay-wlan/\{profile\}", "profile", "WLAN", "name", Confidence.HIGH,
     "Path /overlay-wlan/{profile} — overlay WLAN profile name"),

    # ── Tunnel paths ─────────────────────────────────────────────
    (r"/tunnels?/\{name\}", "name", "Tunnel", "name", Confidence.EXACT,
     "Path /tunnels/{name} — refers to a tunnel"),
    (r"/tunnel-groups?/\{name\}", "name", "Tunnel", "name", Confidence.HIGH,
     "Path /tunnel-groups/{name} — refers to a tunnel group"),

    # ── VLAN paths ───────────────────────────────────────────────
    (r"/(?:vlans?|named-vlan|layer2-vlan)/\{(?:name|vlan)\}", "name",
     "VLAN", "name", Confidence.EXACT,
     "Path /vlans/{name} — refers to a VLAN"),
    (r"/(?:vlans?|named-vlan|layer2-vlan)/\{(?:name|vlan)\}", "vlan",
     "VLAN", "name", Confidence.EXACT,
     "Path /layer2-vlan/{vlan} — refers to a VLAN"),

    # ── Cluster paths ────────────────────────────────────────────
    (r"/(?:gateway-clusters?|clusters?|swarms)/\{(?:name|cluster-name)\}",
     "name", "Cluster", "name", Confidence.EXACT,
     "Path /gateway-clusters/{name} — refers to a cluster"),
    (r"/swarms/\{cluster-id\}", "cluster-id", "Cluster", "id", Confidence.EXACT,
     "Path /swarms/{cluster-id} — refers to a cluster"),

    # ── Policy / security paths ──────────────────────────────────
    (r"/roles?/\{name\}", "name", "Policy", "name", Confidence.HIGH,
     "Path /roles/{name} — refers to a role/policy"),
    (r"/(?:policies|acls?|firewall|role-acls?|role-gpids?|object-groups?|"
     r"airgroup-policies)/\{name\}", "name", "Policy", "name", Confidence.HIGH,
     "Path /policies/{name} — refers to a policy/ACL"),
    (r"/authz-policies/\{policy-id\}", "policy-id", "Policy", "id", Confidence.EXACT,
     "Path /authz-policies/{policy-id} — refers to a policy"),

    # ── Port / interface paths ───────────────────────────────────
    (r"/(?:ethernet-)?interfaces?/\{name\}", "name", "Port", "name", Confidence.HIGH,
     "Path /interfaces/{name} — refers to an interface/port"),
    (r"/sub-interfaces/\{parent-name-id\}", "parent-name-id", "Port", "name",
     Confidence.HIGH,
     "Path /sub-interfaces/{parent-name-id} — parent interface reference"),

    # ── Profile paths (explicit port/switch/device profiles) ─────
    (r"/(?:sw-port-profiles?|gw-port-profiles?|ap-port-profiles?)/\{(?:name|profile-name)\}",
     "name", "ConfigProfile", "name", Confidence.EXACT,
     "Path /port-profiles/{name} — refers to a config profile"),
    (r"/(?:sw-port-profiles?|gw-port-profiles?|ap-port-profiles?)/\{(?:name|profile-name)\}",
     "profile-name", "ConfigProfile", "name", Confidence.EXACT,
     "Path /port-profiles/{profile-name} — refers to a config profile"),
    (r"/switch-profiles?/\{name\}", "name", "ConfigProfile", "name", Confidence.EXACT,
     "Path /switch-profiles/{name} — refers to a switch config profile"),
    (r"/device-profile/\{name\}", "name", "ConfigProfile", "name", Confidence.EXACT,
     "Path /device-profile/{name} — refers to a device config profile"),
    (r"/interface-profiles?/\{name\}", "name", "ConfigProfile", "name", Confidence.EXACT,
     "Path /interface-profiles/{name} — refers to an interface profile"),

    # ── Config-specific named resources (QoS, routing, etc.) ─────
    (r"/qos-queues/\{q-profile-name\}", "q-profile-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /qos-queues/{q-profile-name} — QoS queue profile"),
    (r"/qos-schedules/\{sched-profile-name\}", "sched-profile-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /qos-schedules/{sched-profile-name} — QoS scheduler profile"),
    (r"/qos-thresholds/\{thresh-profile-name\}", "thresh-profile-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /qos-thresholds/{thresh-profile-name} — QoS threshold profile"),
    (r"/route-maps?/\{route-map-name\}", "route-map-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /route-maps/{route-map-name} — route map config"),
    (r"/aspath-lists?/\{aspath-list-name\}", "aspath-list-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /aspath-lists/{aspath-list-name} — AS-path list config"),
    (r"/community-lists?/\{community-list-name-community-type\}",
     "community-list-name-community-type", "ConfigProfile", "name", Confidence.EXACT,
     "Path /community-lists/{...} — community list config"),
    (r"/prefix-lists?/\{prefix-list-name-address-family\}",
     "prefix-list-name-address-family", "ConfigProfile", "name", Confidence.EXACT,
     "Path /prefix-lists/{...} — prefix list config"),
    (r"/nae-agents?/\{agent-name\}", "agent-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /nae-agents/{agent-name} — NAE agent config"),
    (r"/switch-chassis/\{chassis-name\}", "chassis-name",
     "ConfigProfile", "name", Confidence.EXACT,
     "Path /switch-chassis/{chassis-name} — chassis config"),
    (r"/overrides?/\{override-id\}", "override-id",
     "ConfigProfile", "id", Confidence.EXACT,
     "Path /overrides/{override-id} — config override"),
    (r"/portals?/\{portal-id\}", "portal-id",
     "ConfigProfile", "id", Confidence.EXACT,
     "Path /portals/{portal-id} — captive portal config"),
    (r"/skins?/\{skin-id\}", "skin-id",
     "ConfigProfile", "id", Confidence.EXACT,
     "Path /skins/{skin-id} — portal skin config"),
    (r"/message-providers?/\{provider-id\}", "provider-id",
     "ConfigProfile", "id", Confidence.EXACT,
     "Path /message-providers/{provider-id} — message provider config"),
    (r"/container-networks?/\{name-vrf\}", "name-vrf",
     "ConfigProfile", "name", Confidence.HIGH,
     "Path /container-networks/{name-vrf} — container network VRF config"),
    (r"/tracking-object/\{identifier\}", "identifier",
     "ConfigProfile", "id", Confidence.HIGH,
     "Path /tracking-object/{identifier} — tracking object config"),
    (r"/config-assignments?/.*\{profile-type\}", "profile-type",
     "ConfigProfile", "category", Confidence.HIGH,
     "Path /config-assignments/.../{profile-type} — config profile type"),
    (r"/config-assignments?/.*\{profile-instance\}", "profile-instance",
     "ConfigProfile", "name", Confidence.HIGH,
     "Path /config-assignments/.../{profile-instance} — config profile instance"),

    # ── Auth / certificate / security config paths ───────────────
    (r"/(?:auth-servers?|auth-server-global-config|aaa-profile|"
     r"auth-survivability|auth-profiles?)/\{(?:name|auth-profile-id)\}",
     "name", "ConfigProfile", "name", Confidence.HIGH,
     "Path /auth-.../{name} — auth/AAA config resource"),
    (r"/auth-profiles/\{auth-profile-id\}", "auth-profile-id",
     "Policy", "id", Confidence.EXACT,
     "Path /auth-profiles/{auth-profile-id} — auth profile ID"),
    (r"/(?:certificates?|certificate-(?:store|usage|rcp)|device-certificates|"
     r"gw-certificate-usage|est-profiles?)/\{name\}", "name",
     "ConfigProfile", "name", Confidence.HIGH,
     "Path /certificates/{name} — certificate/PKI config resource"),

    # ── Reporting paths ──────────────────────────────────────────
    (r"/reports?/.*\{report-run-id\}", "report-run-id",
     "Report", "id", Confidence.EXACT,
     "Path /reports/../{report-run-id} — report run ID"),
    (r"/sitemaps?/.*\{building-id\}", "building-id",
     "Floor", "id", Confidence.HIGH,
     "Path /sitemaps/.../{building-id} — building in site map"),
    (r"/ap-ranging-scans?/\{scan-id\}", "scan-id",
     "Task", "id", Confidence.HIGH,
     "Path /ap-ranging-scans/{scan-id} — AP ranging scan task"),
    (r"/device-locations?/\{location-id\}", "location-id",
     "Site", "scope_id", Confidence.HIGH,
     "Path /device-locations/{location-id} — device location"),
    (r"/asset-tags?/\{asset-tag-id\}", "asset-tag-id",
     "Device", "id", Confidence.HIGH,
     "Path /asset-tags/{asset-tag-id} — asset tag for a device"),
    (r"/import/\{import-id\}", "import-id",
     "Task", "id", Confidence.HIGH,
     "Path /import/{import-id} — import task"),
    (r"/cnac-image/.*\{image-id\}", "image-id",
     "ConfigProfile", "id", Confidence.HIGH,
     "Path /cnac-image/{image-id} — CNAC image resource"),
    (r"/cnac-job/\{job-id\}", "job-id",
     "Task", "id", Confidence.EXACT,
     "Path /cnac-job/{job-id} — CNAC job task"),

    # ── GreenLake platform paths ─────────────────────────────────
    (r"/webhooks?/\{id\}", "id", "Webhook", "id", Confidence.EXACT,
     "Path /webhooks/{id} — webhook resource"),
    (r"/workspaces?/\{workspaceId\}", "workspaceId",
     "Workspace", "id", Confidence.EXACT,
     "Path /workspaces/{workspaceId} — GreenLake workspace"),
    (r"/credentials?/\{credentialId\}", "credentialId",
     "Credential", "id", Confidence.EXACT,
     "Path /credentials/{credentialId} — GreenLake credential"),
    (r"/msp-tenants?/\{tenantId\}", "tenantId",
     "Org", "scope_id", Confidence.HIGH,
     "Path /msp-tenants/{tenantId} — MSP tenant (maps to Org)"),
    (r"/delivery-failures?/\{failureId\}", "failureId",
     "Webhook", "id", Confidence.MEDIUM,
     "Path /delivery-failures/{failureId} — webhook delivery failure"),
    (r"/service-provisions?/\{id\}", "id",
     "Subscription", "id", Confidence.EXACT,
     "Path /service-provisions/{id} — service provision/subscription"),

    # ── Catch-all: /network-config/v1*/RESOURCE/{name} ───────────
    # This covers 100+ config resource paths like /bgp/{name}, /dns/{name},
    # /snmp/{name}, /stp/{name}, etc. — all are named config resources.
    # MUST be LAST among {name} rules to not shadow specific matches above.
    (r"/network-config/v\d[^/]*/[^/]+/\{name\}", "name",
     "ConfigProfile", "name", Confidence.HIGH,
     "Path /network-config/.../RESOURCE/{name} — named config resource"),

    # ── Generic {id} disambiguation ──────────────────────────────
    # Specific {id} rules above take priority; this catches remaining ones.
    (r"/\{id\}(?:/|$)", "id", "", "", Confidence.UNMAPPED,
     "Generic {id} in path — needs deeper analysis"),
]

# ── Category-based disambiguation ────────────────────────────────────
#
# When path patterns are too generic, use the API category to disambiguate.
# Format: (category_pattern_regex, param_name, entity_name, field_name, confidence, reason)

_CATEGORY_RULES: list[tuple[str, str, str, str, Confidence, str]] = [
    (r"(?i)wireless|wlan", "name", "WLAN", "name", Confidence.MEDIUM,
     "Category is wireless-related — {name} likely refers to WLAN"),
    (r"(?i)vlan|network", "name", "VLAN", "name", Confidence.MEDIUM,
     "Category is VLAN/network-related — {name} likely refers to VLAN"),
    (r"(?i)tunnel", "name", "Tunnel", "name", Confidence.MEDIUM,
     "Category is tunnel-related — {name} likely refers to tunnel"),
    (r"(?i)routing|overlay", "name", "Tunnel", "name", Confidence.LOW,
     "Category suggests routing/overlay — {name} might refer to tunnel"),
    (r"(?i)security|role|policy|firewall", "name", "Policy", "name", Confidence.MEDIUM,
     "Category is security-related — {name} likely refers to policy"),
    (r"(?i)scope.*management", "name", "Site", "name", Confidence.MEDIUM,
     "Scope Management category — {name} likely refers to a scope entity"),
]


class PatternRuleMapper(Mapper):
    """Maps parameters using path and category pattern matching.

    Resolves ambiguous parameters like {name} and {id} by examining
    the endpoint path structure and API category.
    """

    @property
    def name(self) -> str:
        return "pattern_rules"

    def map_param(self, ctx: ParamContext, registry: EntityRegistry) -> MappingResult:
        endpoint_id = f"{ctx.endpoint_method}:{ctx.endpoint_path}"

        # First try path-based rules
        for path_regex, target_param, entity_name, field_name, confidence, reason in _PATH_RULES:
            if ctx.param_name != target_param:
                continue
            if re.search(path_regex, ctx.endpoint_path):
                if confidence == Confidence.UNMAPPED:
                    continue  # Rule explicitly says "can't decide"
                entity = registry.get(entity_name)
                if entity is None:
                    continue
                return MappingResult(
                    param_name=ctx.param_name,
                    param_location=ctx.param_location,
                    entity_name=entity_name,
                    field_name=field_name,
                    confidence=confidence,
                    mapper_name=self.name,
                    reason=reason,
                    endpoint_id=endpoint_id,
                )

        # Then try category-based rules
        for cat_regex, target_param, entity_name, field_name, confidence, reason in _CATEGORY_RULES:
            if ctx.param_name != target_param:
                continue
            if re.search(cat_regex, ctx.endpoint_category):
                entity = registry.get(entity_name)
                if entity is None:
                    continue
                return MappingResult(
                    param_name=ctx.param_name,
                    param_location=ctx.param_location,
                    entity_name=entity_name,
                    field_name=field_name,
                    confidence=confidence,
                    mapper_name=self.name,
                    reason=reason,
                    endpoint_id=endpoint_id,
                )

        return MappingResult(
            param_name=ctx.param_name,
            param_location=ctx.param_location,
            confidence=Confidence.UNMAPPED,
            mapper_name=self.name,
            endpoint_id=endpoint_id,
        )
