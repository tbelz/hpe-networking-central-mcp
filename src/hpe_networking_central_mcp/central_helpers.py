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
import sys
import threading
import time
from datetime import datetime, timezone

import httpx

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"


# ── Error hierarchy ──────────────────────────────────────────────────


class CentralAPIError(Exception):
    """Base exception for Central API errors with structured error details."""

    def __init__(self, status_code: int, error_code: str = "", message: str = "", debug_id: str = ""):
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.debug_id = debug_id
        super().__init__(
            f"[{status_code}] {error_code}: {message}" if error_code else f"[{status_code}] {message}"
        )


class AuthenticationError(CentralAPIError):
    """401/403 authentication or authorization failure."""


class RateLimitError(CentralAPIError):
    """429 rate limit exceeded (after retry exhaustion)."""


class NotFoundError(CentralAPIError):
    """404 resource not found."""


class PaginationError(CentralAPIError):
    """Error during paginated fetch."""


class CentralAPI:
    """Pre-authenticated HTTP client for Central API (Level 2 Smart Client).

    Reads credentials from environment variables injected by the MCP server.
    Handles token acquisition, 401 retry, 429 rate-limit retry, and
    structured error parsing transparently.
    """

    _MAX_RATE_LIMIT_WAIT = 60  # seconds

    def __init__(self) -> None:
        self._base_url = os.environ["CENTRAL_BASE_URL"].rstrip("/")
        self._client_id = os.environ["CENTRAL_CLIENT_ID"]
        self._client_secret = os.environ["CENTRAL_CLIENT_SECRET"]
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._lock = threading.Lock()
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
        with self._lock:
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

        # 429 rate limit — retry once after waiting
        if resp.status_code == 429:
            wait = _parse_retry_wait(resp)
            if wait > self._MAX_RATE_LIMIT_WAIT:
                err = _parse_error_body(resp)
                raise RateLimitError(
                    429, err.get("errorCode", ""),
                    f"Rate limited, retry after {wait}s exceeds {self._MAX_RATE_LIMIT_WAIT}s cap",
                    err.get("debugId", ""),
                )
            print(f"Rate limited, waiting {wait:.0f}s before retry...", file=sys.stderr)
            time.sleep(wait)
            resp = self._http.request(
                method, url,
                params=params,
                json=json_body,
                headers={"Authorization": f"Bearer {self._access_token}", "Accept": "application/json"},
            )
            if resp.status_code == 429:
                err = _parse_error_body(resp)
                raise RateLimitError(429, err.get("errorCode", ""), "Rate limited after retry", err.get("debugId", ""))

        # 401 — refresh token and retry once
        if resp.status_code == 401 and _retry:
            with self._lock:
                self._access_token = ""
                self._token_expires_at = 0.0
            return self._request(method, path, params=params, json_body=json_body, _retry=False)

        # Structured error handling for non-2xx responses
        if resp.status_code >= 400:
            err = _parse_error_body(resp)
            status = resp.status_code
            error_code = err.get("errorCode", "")
            message = err.get("message", resp.text[:200])
            debug_id = err.get("debugId", "")
            if status in (401, 403):
                raise AuthenticationError(status, error_code, message, debug_id)
            if status == 404:
                raise NotFoundError(status, error_code, message, debug_id)
            raise CentralAPIError(status, error_code, message, debug_id)

        return resp.json()

    def paginate(
        self,
        path: str,
        params: dict | None = None,
        *,
        max_pages: int = 50,
        page_size: int = 100,
        item_key: str | None = None,
    ) -> list[dict]:
        """Fetch all pages of a paginated list endpoint.

        Auto-detects cursor-based (MRT APIs with ``next`` token) vs
        offset-based (Config APIs with ``offset`` integer) pagination
        from the first response.

        Args:
            path: API path (e.g. "network-monitoring/v1alpha1/devices").
            params: Extra query parameters (merged with pagination params).
            max_pages: Safety limit on number of pages fetched (default 50).
            page_size: Items per page (default 100).
            item_key: Response key containing the item array. Auto-detected
                      if not provided (tries "items", then first list-valued key).

        Returns:
            Flat list of all items across all pages.

        Raises:
            PaginationError: On unexpected response structure or max_pages exceeded.
        """
        all_items: list[dict] = []
        merged = dict(params or {})
        merged["limit"] = str(page_size)

        # First request — also determines pagination style
        try:
            resp = self._request("GET", path, params=merged)
        except CentralAPIError as exc:
            raise PaginationError(
                exc.status_code, exc.error_code,
                f"Pagination failed on first page: {exc.message}", exc.debug_id,
            ) from exc

        if not isinstance(resp, dict):
            raise PaginationError(0, "", f"Expected dict response, got {type(resp).__name__}")

        # Detect item key
        key = item_key or _detect_item_key(resp)
        if key is None:
            raise PaginationError(0, "", f"Cannot detect item array in response keys: {list(resp.keys())}")

        items = resp.get(key, [])
        all_items.extend(items)
        total = resp.get("total", 0)

        # Detect pagination style from the first response
        style = "cursor" if resp.get("next") is not None else "offset"

        for page_num in range(2, max_pages + 1):
            if total and len(all_items) >= total:
                break
            if not items:
                break

            page_params = dict(merged)
            if style == "cursor":
                cursor = resp.get("next")
                if not cursor:
                    break
                page_params["next"] = str(cursor)
            else:
                page_params["offset"] = str(len(all_items))

            try:
                resp = self._request("GET", path, params=page_params)
            except CentralAPIError as exc:
                raise PaginationError(
                    exc.status_code, exc.error_code,
                    f"Pagination failed on page {page_num}: {exc.message}", exc.debug_id,
                ) from exc

            items = resp.get(key, [])
            all_items.extend(items)
        else:
            if total and len(all_items) < total:
                print(
                    f"Warning: paginate() hit max_pages={max_pages} "
                    f"with {len(all_items)}/{total} items",
                    file=sys.stderr,
                )

        return all_items


# ── Module-level helpers ─────────────────────────────────────────────


def _parse_error_body(resp: httpx.Response) -> dict:
    """Try to parse the standard Central error JSON body."""
    try:
        return resp.json()
    except Exception:
        return {}


def _parse_retry_wait(resp: httpx.Response) -> float:
    """Extract wait time from rate-limit response headers."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 1.0)
        except ValueError:
            pass

    reset = resp.headers.get("X-RateLimit-Reset")
    if reset:
        try:
            reset_time = datetime.fromisoformat(reset)
            if reset_time.tzinfo is None:
                reset_time = reset_time.replace(tzinfo=timezone.utc)
            wait = (reset_time - datetime.now(timezone.utc)).total_seconds()
            return max(wait, 1.0)
        except (ValueError, TypeError):
            pass

    return 5.0  # default fallback


def _detect_item_key(resp: dict) -> str | None:
    """Detect the key containing the items array in a paginated response."""
    if "items" in resp and isinstance(resp["items"], list):
        return "items"
    for key, value in resp.items():
        if isinstance(value, list) and key not in ("errors",):
            return key
    return None


# Module-level singleton — ready to use on import
api = CentralAPI()
