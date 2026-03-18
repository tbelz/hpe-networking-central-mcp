"""Configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Server settings loaded from environment variables."""

    # Central API credentials
    central_base_url: str = ""
    central_client_id: str = ""
    central_client_secret: str = ""

    # GLP credentials (default to Central creds)
    glp_client_id: str = ""
    glp_client_secret: str = ""

    # Paths
    script_library_path: Path = field(default_factory=lambda: Path("/scripts/library"))
    docs_path: Path = field(default_factory=lambda: Path("/docs"))

    # Inventory cache TTL in seconds
    inventory_cache_ttl: int = 300

    # OpenAPI spec cache
    spec_cache_dir: Path = field(default_factory=lambda: Path("/data/oas_cache"))
    spec_cache_ttl: int = 86400  # 24 hours

    @property
    def has_credentials(self) -> bool:
        return bool(self.central_base_url and self.central_client_id and self.central_client_secret)

    @property
    def effective_glp_client_id(self) -> str:
        return self.glp_client_id or self.central_client_id

    @property
    def effective_glp_client_secret(self) -> str:
        return self.glp_client_secret or self.central_client_secret


def load_settings() -> Settings:
    """Load settings from environment variables."""
    return Settings(
        central_base_url=os.environ.get("CENTRAL_BASE_URL", "").strip().rstrip("/"),
        central_client_id=os.environ.get("CENTRAL_CLIENT_ID", "").strip(),
        central_client_secret=os.environ.get("CENTRAL_CLIENT_SECRET", "").strip(),
        glp_client_id=os.environ.get("GLP_CLIENT_ID", "").strip(),
        glp_client_secret=os.environ.get("GLP_CLIENT_SECRET", "").strip(),
        script_library_path=Path(os.environ.get("SCRIPT_LIBRARY_PATH", "/scripts/library")),
        docs_path=Path(os.environ.get("DOCS_PATH", "/docs")),
        inventory_cache_ttl=int(os.environ.get("INVENTORY_CACHE_TTL", "300")),
        spec_cache_dir=Path(os.environ.get("SPEC_CACHE_DIR", "/data/oas_cache")),
        spec_cache_ttl=int(os.environ.get("SPEC_CACHE_TTL", "86400")),
    )
