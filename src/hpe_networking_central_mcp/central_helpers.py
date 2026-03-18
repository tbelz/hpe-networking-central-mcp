"""Pre-authenticated API helper for scripts executed by the MCP server.

This module is copied into the script library at server startup so that
scripts can simply ``from central_helpers import api`` and make API calls
without any OAuth2 boilerplate.

Usage inside a script::

    from central_helpers import api

    devices = api.get("network-monitoring/v1alpha1/devices", params={"limit": "100"})
    api.post("network-config/v1alpha1/dhcp-pool", json_body={"name": "pool1", ...})
"""

from __future__ import annotations

import os
import time

import httpx

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"


class CentralAPI:
    """Thin authenticated HTTP client for Central API.

    Reads credentials from environment variables injected by the MCP server.
    Handles token acquisition and 401 retry transparently.
    """

    def __init__(self) -> None:
        self._base_url = os.environ["CENTRAL_BASE_URL"].rstrip("/")
        self._client_id = os.environ["CENTRAL_CLIENT_ID"]
        self._client_secret = os.environ["CENTRAL_CLIENT_SECRET"]
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._http = httpx.Client(timeout=30.0)

    # -- public methods ------------------------------------------------

    def get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: dict | None = None, params: dict | None = None) -> dict:
        return self._request("POST", path, params=params, json_body=json_body)

    def patch(self, path: str, json_body: dict | None = None, params: dict | None = None) -> dict:
        return self._request("PATCH", path, params=params, json_body=json_body)

    def put(self, path: str, json_body: dict | None = None, params: dict | None = None) -> dict:
        return self._request("PUT", path, params=params, json_body=json_body)

    def delete(self, path: str, params: dict | None = None) -> dict:
        return self._request("DELETE", path, params=params)

    # -- internals -----------------------------------------------------

    def _ensure_token(self) -> None:
        if self._access_token and time.time() < self._token_expires_at:
            return
        resp = self._http.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self._client_id, self._client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        body = resp.json()
        self._access_token = body["access_token"]
        self._token_expires_at = time.time() + int(body.get("expires_in", 7200)) - 60

    def _request(self, method: str, path: str, params=None, json_body=None, *, _retry=True) -> dict:
        self._ensure_token()
        url = f"{self._base_url}/{path.lstrip('/')}"
        resp = self._http.request(
            method, url,
            params=params,
            json=json_body,
            headers={"Authorization": f"Bearer {self._access_token}", "Accept": "application/json"},
        )
        if resp.status_code == 401 and _retry:
            self._access_token = ""
            self._token_expires_at = 0.0
            return self._request(method, path, params=params, json_body=json_body, _retry=False)
        resp.raise_for_status()
        return resp.json()


# Module-level singleton — ready to use on import
api = CentralAPI()
