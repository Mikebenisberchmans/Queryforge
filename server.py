#!/usr/bin/env python3
"""
Unified MCP Server
Combines SQLite database tools + Streamlit dashboard tools in one FastMCP server.
The idea: query your DB, then build a live dashboard from the same server.
"""

import os
import re
import sys
import json
import signal
import sqlite3
import subprocess
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
DASHBOARD_FILE = BASE_DIR / "dashboard.py"
VENV_PYTHON   = sys.executable
DB_PATH       = Path(os.environ.get("DB_PATH", "./database.db"))
PORT          = int(os.environ.get("DASHBOARD_PORT", 8501))

# ── State ─────────────────────────────────────────────────────────────────────
streamlit_process: subprocess.Popen | None = None

# ── MCP Server ────────────────────────────────────────────────────────────────
mcp = FastMCP("sqlite-dashboard-server")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         SQLITE HELPERS                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

BLOCKED_SQL = re.compile(
    r"^\s*(DROP|DELETE|TRUNCATE|ALTER|INSERT|UPDATE|CREATE|REPLACE|ATTACH|DETACH)\b",
    re.IGNORECASE,
)

def _is_read_only(sql: str) -> bool:
    return not BLOCKED_SQL.match(sql.strip())

def _valid_table_name(name: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9_]+$", name))

def _get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def _rows(cursor) -> list[dict]:
    return [dict(row) for row in cursor.fetchall()]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         DASHBOARD HELPERS                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

STDLIB = {
    "os", "sys", "re", "json", "math", "time", "datetime", "random",
    "pathlib", "collections", "itertools", "functools", "typing",
    "io", "csv", "string", "hashlib", "copy", "enum", "abc", "sqlite3",
}

def _extract_imports(code: str) -> list[str]:
    found = set()
    for m in re.finditer(r"^(?:import|from)\s+([a-zA-Z_][a-zA-Z0-9_]*)", code, re.MULTILINE):
        pkg = m.group(1)
        if pkg not in STDLIB:
            found.add(pkg)
    return list(found)

def _install(packages: list[str]) -> str:
    if not packages:
        return "No packages to install."
    r = subprocess.run(
        [VENV_PYTHON, "-m", "pip", "install", *packages, "--quiet"],
        capture_output=True, text=True
    )
    return f"Installed: {', '.join(packages)}" if r.returncode == 0 else f"pip error:\n{r.stderr}"

def _start_streamlit() -> str:
    global streamlit_process
    if not DASHBOARD_FILE.exists():
        return "No dashboard.py found. Create one first."
    if streamlit_process and streamlit_process.poll() is None:
        streamlit_process.terminate()
        streamlit_process.wait()
    streamlit_process = subprocess.Popen(
        [
            VENV_PYTHON, "-m", "streamlit", "run", str(DASHBOARD_FILE),
            "--server.port", str(PORT),
            "--server.headless", "true",
            "--server.runOnSave", "true",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return f"Streamlit running at http://localhost:{PORT}"


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         SQLITE TOOLS                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@mcp.tool()
def list_tables() -> str:
    """List all tables in the SQLite database."""
    try:
        conn = _get_conn()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        conn.close()
        tables = [r["name"] for r in rows]
        return json.dumps({"tables": tables}, indent=2)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def describe_table(table: str) -> str:
    """
    Show column names, types, and constraints for a table.
    Use before writing queries to understand the schema.
    
    Args:
        table: Name of the table to describe
    """
    if not _valid_table_name(table):
        return "Error: Invalid table name."
    try:
        conn = _get_conn()
        cols = _rows(conn.execute(f"PRAGMA table_info({table})"))
        conn.close()
        if not cols:
            return f"Error: Table '{table}' does not exist."
        return json.dumps({"table": table, "columns": cols}, indent=2)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def sample_table(table: str, limit: int = 5) -> str:
    """
    Return the first N rows of a table (default 5, max 50).
    
    Args:
        table: Name of the table to sample
        limit: Number of rows to return (default 5, max 50)
    """
    if not _valid_table_name(table):
        return "Error: Invalid table name."
    limit = min(max(1, limit), 50)
    try:
        conn = _get_conn()
        rows = _rows(conn.execute(f"SELECT * FROM {table} LIMIT ?", (limit,)))
        conn.close()
        return json.dumps({"table": table, "row_count": len(rows), "rows": rows}, indent=2, default=str)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def query_database(sql: str) -> str:
    """
    Run a SELECT query against the SQLite database.
    Only read-only SELECT statements are allowed.
    Explore schema first with list_tables and describe_table.
    
    Args:
        sql: A valid SQLite SELECT statement
    """
    if not _is_read_only(sql):
        return "Error: Only SELECT statements are permitted. Write operations are blocked."
    try:
        conn = _get_conn()
        rows = _rows(conn.execute(sql))
        conn.close()
        return json.dumps({"row_count": len(rows), "rows": rows}, indent=2, default=str)
    except sqlite3.OperationalError as e:
        return f"SQL error: {e}"
    except Exception as e:
        return f"Error: {e}"


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         DASHBOARD TOOLS                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@mcp.tool()
def create_dashboard(code: str) -> str:
    """
    Creates or updates the Streamlit dashboard with the given Python code.
    Automatically installs missing dependencies and starts/reloads the app.
    Tip: use sqlite3 directly in the dashboard code with DB_PATH env var.
    
    Args:
        code: Full Python source code for the Streamlit dashboard
    """
    packages = _extract_imports(code)
    install_log = _install(packages)
    DASHBOARD_FILE.write_text(code, encoding="utf-8")

    global streamlit_process
    if streamlit_process is None or streamlit_process.poll() is not None:
        status = _start_streamlit()
    else:
        status = f"Dashboard updated — auto-reloaded at http://localhost:{PORT}"

    return "\n".join([
        f"✅ dashboard.py written ({len(code.splitlines())} lines)",
        f"📦 {install_log}",
        f"🚀 {status}",
    ])


@mcp.tool()
def stop_dashboard() -> str:
    """Stops the running Streamlit dashboard process."""
    global streamlit_process
    if streamlit_process is None or streamlit_process.poll() is not None:
        return "No dashboard is currently running."
    streamlit_process.terminate()
    streamlit_process.wait()
    streamlit_process = None
    return "✅ Streamlit dashboard stopped."


@mcp.tool()
def get_dashboard_status() -> str:
    """Returns whether the Streamlit dashboard is running and on which port."""
    global streamlit_process
    if streamlit_process and streamlit_process.poll() is None:
        return f"🟢 Running at http://localhost:{PORT} (PID {streamlit_process.pid})"
    return "🔴 Not running."


@mcp.tool()
def read_dashboard() -> str:
    """Returns the current contents of dashboard.py."""
    if not DASHBOARD_FILE.exists():
        return "No dashboard.py exists yet."
    return DASHBOARD_FILE.read_text(encoding="utf-8")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                         CLEANUP & ENTRY                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _cleanup(sig, frame):
    global streamlit_process
    if streamlit_process and streamlit_process.poll() is None:
        streamlit_process.terminate()
    sys.exit(0)

signal.signal(signal.SIGINT, _cleanup)
signal.signal(signal.SIGTERM, _cleanup)


if __name__ == "__main__":
    print(f"🚀 Unified SQLite+Dashboard MCP Server starting...", file=sys.stderr)
    print(f"   DB_PATH        = {DB_PATH}", file=sys.stderr)
    print(f"   DASHBOARD_FILE = {DASHBOARD_FILE}", file=sys.stderr)
    print(f"   DASHBOARD_PORT = {PORT}", file=sys.stderr)
    mcp.run(transport="stdio")