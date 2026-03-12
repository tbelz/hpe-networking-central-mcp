"""Lightweight HTTP client for HPE Aruba Networking Central API.

Handles OAuth2 client-credentials authentication and token refresh.
Uses httpx instead of pycentral for a single, minimal HTTP stack.
"""

from __future__ import annotations

import time

import httpx
import structlog

logger = structlog.get_logger("central_client")

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"


class CentralClient:
    """HTTP client for Central API with OAuth2 token lifecycle."""

    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._http = httpx.Client(timeout=30.0)

    def _ensure_token(self) -> None:
        """Generate or refresh the OAuth2 access token if expired."""
        if self._access_token and time.time() < self._token_expires_at:
            return

        logger.info("token_refresh_start")
        resp = self._http.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self._client_id, self._client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        body = resp.json()
        self._access_token = body["access_token"]
        # Refresh 60s before expiry; default to 7200s if not provided
        expires_in = int(body.get("expires_in", 7200))
        self._token_expires_at = time.time() + expires_in - 60
        logger.info("token_refresh_done", expires_in=expires_in)

    def get(self, path: str, params: dict[str, str] | None = None) -> dict:
        """Make an authenticated GET request to Central API.

        Args:
            path: API path (e.g., "network-monitoring/v1alpha1/devices").
            params: Optional query parameters.

        Returns:
            Parsed JSON response body.

        Raises:
            httpx.HTTPStatusError: On non-2xx responses (after one token-refresh retry on 401).
        """
        return self._request("GET", path, params=params)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        json_body: dict | None = None,
        *,
        _retry: bool = True,
    ) -> dict:
        """Internal request method with auto-retry on 401."""
        self._ensure_token()
        url = f"{self._base_url}/{path.lstrip('/')}"
        resp = self._http.request(
            method,
            url,
            params=params,
            json=json_body,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Accept": "application/json",
            },
        )

        # Retry once on 401 (token may have been revoked server-side)
        if resp.status_code == 401 and _retry:
            logger.info("token_expired_retry")
            self._access_token = ""
            self._token_expires_at = 0.0
            return self._request(method, path, params=params, json_body=json_body, _retry=False)

        resp.raise_for_status()
        return resp.json()

    def validate(self) -> None:
        """Test credentials by requesting an OAuth2 token.

        Raises:
            httpx.HTTPStatusError: If the token request fails (bad credentials).
        """
        self._ensure_token()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()
