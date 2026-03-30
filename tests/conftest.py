"""Shared fixtures for seed integration tests.

Spins up a real temp LadybugDB graph + IPC server so seed scripts run
as subprocesses — identical to the production execution path in server.py.
"""

from __future__ import annotations

import graphlib
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

# ── Importable package components ───────────────────────────────────
# We add the src dir to sys.path so pytest can import the package
# without requiring an editable install.
import sys

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from hpe_networking_central_mcp.config import Settings, load_settings  # noqa: E402
from hpe_networking_central_mcp.graph.ipc_server import GraphIPCServer  # noqa: E402
from hpe_networking_central_mcp.graph.manager import GraphManager  # noqa: E402


# ── Dataclasses ─────────────────────────────────────────────────────

@dataclass
class SeedResult:
    """Result of running a seed script as a subprocess."""
    exit_code: int
    stdout: str
    stderr: str
    duration: float


@dataclass
class SeedInfra:
    """All the infrastructure required to run seed scripts."""
    settings: Settings
    graph_manager: GraphManager
    ipc_server: GraphIPCServer
    lib_path: Path
    socket_path: Path

    def run_seed(self, filename: str, parameters: dict[str, str] | None = None) -> SeedResult:
        """Execute a seed script as a subprocess, matching production _run_script()."""
        script_path = self.lib_path / filename
        assert script_path.exists(), f"Seed '{filename}' not found in {self.lib_path}"

        cmd = ["python3", str(script_path)]
        if parameters:
            for key, value in parameters.items():
                cmd.extend([f"--{key}", str(value)])

        env = os.environ.copy()
        env["CENTRAL_BASE_URL"] = self.settings.central_base_url
        env["CENTRAL_CLIENT_ID"] = self.settings.central_client_id
        env["CENTRAL_CLIENT_SECRET"] = self.settings.central_client_secret
        env["GLP_CLIENT_ID"] = self.settings.effective_glp_client_id
        env["GLP_CLIENT_SECRET"] = self.settings.effective_glp_client_secret
        env["GREENLAKE_CLIENT_ID"] = self.settings.effective_glp_client_id
        env["GREENLAKE_CLIENT_SECRET"] = self.settings.effective_glp_client_secret
        env["GLP_BASE_URL"] = self.settings.glp_base_url
        env["GRAPH_DB_PATH"] = str(self.settings.graph_db_path)
        env["GRAPH_IPC_SOCKET"] = str(self.socket_path)

        start = time.monotonic()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(self.lib_path),
            env=env,
        )
        duration = round(time.monotonic() - start, 2)

        return SeedResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            duration=duration,
        )

    def seed_order(self) -> list[str]:
        """Return auto_run seed filenames in topological dependency order."""
        auto_seeds: dict[str, list[str]] = {}
        for meta_file in sorted(self.lib_path.glob("*.meta.json")):
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                if meta.get("auto_run"):
                    script_name = meta_file.name.replace(".meta.json", ".py")
                    if (self.lib_path / script_name).exists():
                        auto_seeds[script_name] = meta.get("depends_on", [])
            except Exception:
                continue

        graph: dict[str, set[str]] = {}
        for name, deps in auto_seeds.items():
            graph[name] = {d for d in deps if d in auto_seeds}

        sorter = graphlib.TopologicalSorter(graph)
        return list(sorter.static_order())


# ── Fixtures ────────────────────────────────────────────────────────

def _load_dotenv() -> None:
    """Parse .env from repo root and inject into os.environ."""
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


@pytest.fixture(scope="session")
def seed_infra():
    """Session-scoped fixture that provides a fully initialised seed execution environment.

    Creates a temp LadybugDB, starts an IPC server, and copies seeds + central_helpers.py
    into a temp library directory — matching the server.py startup path exactly.

    Skips all tests if Central credentials are not present in .env.
    """
    _load_dotenv()
    settings = load_settings()
    if not settings.has_credentials:
        pytest.skip("Central credentials not configured — set CENTRAL_BASE_URL etc. in .env")

    pkg_dir = _SRC_DIR / "hpe_networking_central_mcp"
    seeds_dir = pkg_dir / "seeds"

    with TemporaryDirectory(prefix="seed_test_") as tmp:
        tmp_path = Path(tmp)
        db_path = tmp_path / "graph_db"
        socket_path = tmp_path / "test_seed.sock"
        lib_path = tmp_path / "library"
        lib_path.mkdir()

        # Bootstrap graph with full schema
        gm = GraphManager(db_path)
        gm.initialize()
        gm.create_fts_indexes()

        # Start IPC server
        ipc = GraphIPCServer(socket_path, gm)
        ipc.start()

        # Copy central_helpers.py and _http_core.py (both needed in subprocess)
        for helper_name in ("_http_core.py", "central_helpers.py"):
            helpers_src = pkg_dir / helper_name
            shutil.copy2(helpers_src, lib_path / helper_name)

        # Copy all seed scripts and meta files
        for f in seeds_dir.iterdir():
            if f.suffix in (".py", ".json") and f.name != "__init__.py":
                shutil.copy2(f, lib_path / f.name)

        infra = SeedInfra(
            settings=settings,
            graph_manager=gm,
            ipc_server=ipc,
            lib_path=lib_path,
            socket_path=socket_path,
        )

        yield infra

        ipc.stop()
