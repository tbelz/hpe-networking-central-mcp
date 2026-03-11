#!/usr/bin/env python3
"""Get detailed device inventory from HPE Aruba Networking Central.

Retrieves the full device inventory including unassigned/not-yet-onboarded devices.
Based on pycentral v2 MonitoringDevices.get_all_device_inventory().

Reference: pycentral.new_monitoring.devices.MonitoringDevices.get_all_device_inventory()
"""

import argparse
import json
import os
import sys

from pycentral import NewCentralBase
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
    return NewCentralBase(token_info=token_info)


def main():
    parser = argparse.ArgumentParser(description="Get device inventory from Central")
    parser.add_argument("--site-assigned", choices=["ASSIGNED", "UNASSIGNED"],
                        default=None, help="Filter by site assignment status")
    parser.add_argument("--search", default=None, help="Search string to filter results")
    parser.add_argument("--serial", default=None, help="Look up a specific device by serial")
    args = parser.parse_args()

    central = get_connection()

    if args.serial:
        # Look up single device
        devices = MonitoringDevices.get_all_device_inventory(central, search=args.serial)
        matching = [d for d in devices if d.get("serialNumber", "").upper() == args.serial.upper()]
        if matching:
            print(json.dumps({"device": matching[0]}, indent=2))
        else:
            print(json.dumps({"error": f"Device {args.serial} not found"}))
            sys.exit(1)
    else:
        devices = MonitoringDevices.get_all_device_inventory(
            central,
            site_assigned=args.site_assigned,
            search=args.search,
        )
        print(json.dumps({"total": len(devices), "devices": devices}, indent=2))


if __name__ == "__main__":
    main()
