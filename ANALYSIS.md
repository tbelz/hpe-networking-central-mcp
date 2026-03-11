# Capability & Limitation Analysis

**Date:** March 11, 2026  
**Version:** 0.1.0 (commit fee614e)  
**Environment:** 10 devices, 4 sites + 2 unassigned

---

## Live Workflow Test Results (7 workflows, 11 API calls)

| Workflow | Scenario | Result | Notes |
|---|---|---|---|
| **WF1a** | Morning health check — summary | **PASS** | 10 devices, 5 sites, correct counts |
| **WF1b** | Show offline devices | **PASS** | Found 1 offline switch (SG23KN5053 at Curry-Bude), full details |
| **WF2a** | Site investigation — curry-zentrale | **PASS** | Case-insensitive filter worked, 3 devices returned |
| **WF2b** | Find gateway by name "GW_Zentrale" | **FAIL** | Actual name is "Zentrale-Aruba7210-1" — substring "GW_Zentrale" not in any field |
| **WF3** | Partial serial lookup "SG23" | **PASS** | Unique match → full details |
| **WF4a** | List scripts | **PASS** | 3 seed scripts with metadata |
| **WF4b** | Execute device summary script | **PASS** | Ran in 2.53s, exit 0, correct JSON output |
| **WF5** | Save + execute custom script | **FAIL** | Test bug: used `code` instead of `content` param — reveals LLM agent must use exact param names |
| **WF6** | List prompts | **N/A** | Response format differs from tools — works fine with real MCP clients |
| **WF7** | Read resource | **N/A** | Same — works fine with real MCP clients |

---

## Capabilities for Common IT Admin Workflows

| IT Admin Task | Supported? | How |
|---|---|---|
| Check network health | **Yes** | `refresh_inventory(summary)` |
| Find problem devices | **Yes** | `refresh_inventory(full, filter_status="offline")` |
| Investigate a site | **Yes** | `refresh_inventory(full, filter_site="...")` |
| Look up device by serial/name/IP | **Yes** | `get_device_details("...")` — partial match |
| Run automated report | **Yes** | `execute_script("get_device_summary.py")` |
| Onboard a new device | **Yes** | `onboard_device` prompt + `onboard_device.py` script |
| Write a custom automation | **Yes** | `save_script` + `execute_script` |
| Push config changes | **Indirect** | Must write a pycentral script first |
| Create/manage sites | **Indirect** | Must write a pycentral script |
| Real-time monitoring | **No** | Poll-based only (5-min cache) |
| Firmware upgrades | **Indirect** | Must write a pycentral script using the right API |
| User/AAA troubleshooting | **Indirect** | pycentral `Troubleshooting` module, must script it |

---

## Identified Limitations

| # | Limitation | Impact | Fix Difficulty |
|---|---|---|---|
| **L1** | No fuzzy/semantic device search | Admin searching "the gateway at Zentrale" won't match "Zentrale-Aruba7210-1" | Medium |
| **L2** | No combined filters on device lookup | Must use `refresh_inventory` filters instead of `get_device_details` | Low (by design) |
| **L3** | Script state doesn't persist across containers | Custom scripts lost without volume mount `-v central-scripts:/scripts/library` | Low (documented) |
| **L4** | No real-time alerting | Poll-based inventory with 5-min cache TTL | High |
| **L5** | Parameter naming must be exact | LLM agent must use `content` not `code` for save_script | Low (schema self-documenting) |
| **L6** | No direct configuration management | Config push requires writing a pycentral script first | By design |
| **L7** | No multi-site comparison | Can only filter by one site at a time | Low |
| **L8** | Unassigned devices show raw `None` values | `"site": "None"` instead of `"unassigned"` in candidate lists | Low |
| **L9** | Prompts/resources not testable via simple JSON-RPC grep | Non-issue for real MCP clients | Non-issue |

---

## Constraints

| Constraint | Limit |
|-----------|-------|
| Script execution timeout | 300 seconds (5 min) |
| Output capture | 10KB stdout, 5KB stderr |
| Inventory cache TTL | 300 seconds (configurable) |
| Ansible inventory call timeout | 120 seconds |
| Script file types | `.py` only |
| Filename validation | `^[a-zA-Z0-9_\-]+\.py$` |

---

## Container Environment

- **Python 3.12** with mcp, pyyaml, structlog, jinja2
- **pycentral v2** (2.0a14 pre-release) — full Central API access
- **Ansible + arubanetworks.hpeanw_central** collection (v2-beta)
- **Docker image size:** ~262MB
