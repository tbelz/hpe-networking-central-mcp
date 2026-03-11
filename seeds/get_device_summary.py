#!/usr/bin/env python3
"""Get a summary of all monitored devices from HPE Aruba Networking Central.

Based on the official pycentral v2 MonitoringDevices workflow.
Retrieves all devices and produces a structured JSON summary grouped by
site, device type, and status.

Reference: pycentral.new_monitoring.devices.MonitoringDevices.get_all_devices()
"""

import argparse
import json
import os
import sys
from collections import defaultdict

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
    parser = argparse.ArgumentParser(description="Get device monitoring summary from Central")
    parser.add_argument("--filter", default=None, help="Optional filter expression for devices")
    parser.add_argument("--sort", default=None, help="Optional sort expression")
    parser.add_argument("--format", choices=["summary", "full"], default="summary",
                        help="Output format: summary (counts) or full (all device data)")
    args = parser.parse_args()

    central = get_connection()
    devices = MonitoringDevices.get_all_devices(central, filter_str=args.filter, sort=args.sort)

    if args.format == "full":
        print(json.dumps({"total": len(devices), "devices": devices}, indent=2))
        return

    # Build summary
    by_site = defaultdict(int)
    by_type = defaultdict(int)
    by_status = defaultdict(int)
    by_model = defaultdict(int)

    for device in devices:
        by_site[device.get("siteName", "unassigned")] += 1
        by_type[device.get("deviceType", "unknown")] += 1
        by_status[device.get("status", "unknown")] += 1
        by_model[device.get("model", "unknown")] += 1

    summary = {
        "total_devices": len(devices),
        "by_site": dict(sorted(by_site.items())),
        "by_type": dict(sorted(by_type.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_model": dict(sorted(by_model.items())),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
