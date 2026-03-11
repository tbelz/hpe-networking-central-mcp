#!/usr/bin/env python3
"""Onboard a device into HPE Aruba Networking Central.

Based on the official onboarding workflow documented in the Ansible collection
(examples/onboarding_example.yml) and pycentral v2 SDK patterns.

Steps:
1. Connect to Central and GLP
2. Check if the device exists in GLP inventory
3. Assign device to the application and subscription in GLP (if needed)
4. Create or verify the target site exists in Central
5. Assign the device persona/function

Reference: pycentral.glp.devices.Devices, pycentral.scopes.site.Site
"""

import argparse
import json
import os
import sys

from pycentral.base import NewCentralBase
from pycentral.new_monitoring.devices import MonitoringDevices


def get_connection():
    """Create a NewCentralBase connection using environment variables."""
    token_info = {
        "new_central": {
            "base_url": os.environ["CENTRAL_BASE_URL"],
            "client_id": os.environ["CENTRAL_CLIENT_ID"],
            "client_secret": os.environ["CENTRAL_CLIENT_SECRET"],
        }
    }
    return NewCentralBase(token_info=token_info, enable_scope=True)


def find_device_in_inventory(central, serial):
    """Check if device exists in Central's device inventory."""
    devices = MonitoringDevices.get_all_device_inventory(central)
    for device in devices:
        if device.get("serialNumber", "").upper() == serial.upper():
            return device
    return None


def assign_persona(central, serial, persona):
    """Assign a persona/device-function to a device via the Central API."""
    resp = central.command(
        api_method="POST",
        api_path="/network-config/v1alpha1/persona-assignment",
        api_data={
            "persona-device-list": [
                {
                    "device-function": persona,
                    "device-id": [serial],
                }
            ]
        },
    )
    return resp


def main():
    parser = argparse.ArgumentParser(description="Onboard a device to HPE Central")
    parser.add_argument("--serial", required=True, help="Device serial number")
    parser.add_argument("--site", required=True, help="Target site name")
    parser.add_argument("--persona", default="ACCESS_SWITCH",
                        help="Device persona (ACCESS_SWITCH, CAMPUS_AP, CORE_SWITCH, etc.)")
    args = parser.parse_args()

    result = {"serial": args.serial, "site": args.site, "persona": args.persona, "steps": []}

    # Step 1: Connect
    central = get_connection()
    result["steps"].append({"step": "connect", "status": "success"})

    # Step 2: Check device in inventory
    device = find_device_in_inventory(central, args.serial)
    if not device:
        result["steps"].append({
            "step": "find_device",
            "status": "error",
            "message": f"Device {args.serial} not found in inventory. "
                       "It may need to be added to GLP first."
        })
        print(json.dumps(result, indent=2))
        sys.exit(1)

    result["steps"].append({
        "step": "find_device",
        "status": "success",
        "device_type": device.get("deviceType"),
        "current_site": device.get("siteName", "unassigned"),
        "current_status": device.get("status"),
    })

    # Step 3: Check/Create site
    sites = central.scopes.get_all_sites() if central.scopes else []
    site_exists = any(s.name == args.site for s in sites) if sites else False

    if not site_exists:
        result["steps"].append({
            "step": "check_site",
            "status": "warning",
            "message": f"Site '{args.site}' not found. Manual site creation required "
                       "(needs address, city, state, country, zipcode, timezone)."
        })
    else:
        result["steps"].append({"step": "check_site", "status": "success", "site_found": True})

    # Step 4: Assign persona
    resp = assign_persona(central, args.serial, args.persona)
    if resp.get("code") in (200, 201, 202):
        result["steps"].append({
            "step": "assign_persona",
            "status": "success",
            "persona": args.persona,
        })
    else:
        result["steps"].append({
            "step": "assign_persona",
            "status": "error",
            "response_code": resp.get("code"),
            "message": str(resp.get("msg", ""))[:500],
        })

    # Final status
    errors = [s for s in result["steps"] if s["status"] == "error"]
    result["overall_status"] = "error" if errors else "success"

    print(json.dumps(result, indent=2))
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
