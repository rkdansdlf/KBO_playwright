"""Detailed diagnostic tool for Oracle OCI Autonomous Database (ADB) connectivity.

Diagnoses:
1. Environment variables (DATABASE_URL, TARGET_DATABASE_URL, TNS_ADMIN)
2. Wallet directory & mandatory files (cwallet.sso, tnsnames.ora, sqlnet.ora)
3. tnsnames.ora parsing & host extraction
4. TCP Port 1522 reachability & DNS resolution
5. python-oracledb driver connect test with detailed error categorization
6. Step-by-step OCI Console troubleshooting runbook
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv

if TYPE_CHECKING:
    from collections.abc import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _check_wallet(tns_admin: str | None) -> tuple[dict[str, bool], list[str], str | None]:
    files_present: dict[str, bool] = {}
    hosts: list[str] = []
    err: str | None = None

    if not tns_admin:
        return files_present, hosts, err

    wallet_path = Path(tns_admin)
    for req_file in ["cwallet.sso", "tnsnames.ora", "sqlnet.ora"]:
        files_present[req_file] = (wallet_path / req_file).is_file()

    tnsnames_file = wallet_path / "tnsnames.ora"
    if tnsnames_file.is_file():
        try:
            content = tnsnames_file.read_text(encoding="utf-8")
            for host_match in re.finditer(r"\(\s*HOST\s*=\s*([^\s)]+)", content, flags=re.IGNORECASE):
                host_part = host_match.group(1).strip()
                if host_part and host_part not in hosts:
                    hosts.append(host_part)
        except (OSError, UnicodeDecodeError, re.error) as e:
            err = f"Failed to parse tnsnames.ora: {e}"

    return files_present, hosts, err


def _check_sockets(hosts: list[str]) -> dict[str, bool]:
    reachability: dict[str, bool] = {}
    for host in hosts:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3.0)
        try:
            res = sock.connect_ex((host, 1522))
            reachability[f"{host}:1522"] = res == 0
        except OSError:
            reachability[f"{host}:1522"] = False
        finally:
            sock.close()
    return reachability


def _try_driver_connect(
    db_url: str,
    tns_admin: str | None,
    wallet_password: str | None,
) -> tuple[bool, str | None, str | None]:
    if not db_url or "oracle" not in db_url.lower():
        return False, None, None

    try:
        import oracledb
    except ImportError as e:
        return False, "DRIVER_NOT_INSTALLED", str(e)

    try:
        from urllib.parse import unquote, urlsplit

        parts = urlsplit(db_url)
        if "@" not in parts.netloc:
            return False, "INVALID_URL", "No credentials or DSN found in URL"

        user = unquote(parts.username or "")
        pwd = unquote(parts.password or "")
        dsn = unquote(parts.netloc.rsplit("@", 1)[1])
        connect_args: dict[str, str] = {"user": user, "password": pwd, "dsn": dsn}
        if tns_admin:
            connect_args["config_dir"] = tns_admin
            connect_args["wallet_location"] = tns_admin
        if wallet_password:
            connect_args["wallet_password"] = wallet_password

        with (
            oracledb.connect(**connect_args) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT 1 FROM DUAL")
            return True, None, None
    except (oracledb.Error, OSError, ValueError, TypeError) as e:
        return False, "ORACLE_CONN_ERROR", str(e)


def diagnose_environment() -> dict[str, Any]:
    """Inspect environment variables, wallet files, and network reachability."""
    tns_admin = os.getenv("TNS_ADMIN")
    db_url = next(
        (
            value
            for key in ("ORACLE_TARGET_URL", "OCI_DB_URL", "TARGET_DATABASE_URL", "DATABASE_URL")
            if (value := os.getenv(key)) and "oracle" in value.lower()
        ),
        "",
    )
    wallet_password = os.getenv("OCI_WALLET_PASSWORD")

    files_present, hosts, parse_err = _check_wallet(tns_admin)
    socket_reachability = _check_sockets(hosts)
    driver_success, err_code, err_msg = _try_driver_connect(db_url, tns_admin, wallet_password)

    diag: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "tns_admin_configured": bool(tns_admin),
        "tns_admin_path": tns_admin or "UNSET",
        "wallet_files_present": files_present,
        "tns_hosts_extracted": hosts,
        "socket_reachability": socket_reachability,
        "driver_connect_attempted": bool(db_url and "oracle" in db_url.lower()),
        "driver_connect_success": driver_success,
        "error_code": err_code,
        "error_message": err_msg or parse_err,
        "troubleshooting_guide": None,
    }

    if not driver_success:
        diag["troubleshooting_guide"] = {
            "step_1_instance_state": "OCI Console -> Oracle Database -> Autonomous Database -> Check lifecycle state is 'Available' (Click 'Start' if stopped).",
            "step_2_network_acl": "OCI Console -> ADB Details -> Network -> Access Control List (ACL) -> Ensure your current client egress IP or 0.0.0.0/0 is whitelisted.",
            "step_3_wallet_mtls": "OCI Console -> ADB Details -> DB Connection -> Download Client Credentials (Wallet) -> Unzip into /Users/mac/keypair/Wallet_... and verify cwallet.sso.",
            "step_4_port_1522": "Verify TCP port 1522 is open on your network firewall without SSL inspection blocking mTLS.",
        }

    return diag


def main(argv: Sequence[str] | None = None) -> int:
    """CLI Entrypoint for OCI connectivity diagnosis."""
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="Diagnose Oracle OCI ADB Connection and Configuration")
    parser.add_argument("--json", action="store_true", default=False, help="Output structured JSON")
    args = parser.parse_args(argv)

    diag = diagnose_environment()

    if args.json:
        sys.stdout.write(json.dumps(diag, indent=2, ensure_ascii=False) + "\n")
    else:
        print("=== Oracle OCI ADB Diagnostic Report ===")
        print(f"TNS_ADMIN: {diag['tns_admin_path']} (Configured: {diag['tns_admin_configured']})")
        print(f"Wallet Files: {diag['wallet_files_present']}")
        print(f"Extracted Hosts: {diag['tns_hosts_extracted']}")
        print(f"Socket Reachability (Port 1522): {diag['socket_reachability']}")
        print(f"Driver Connect Attempted: {diag['driver_connect_attempted']}")
        print(f"Driver Connect Success: {diag['driver_connect_success']}")
        if diag["error_message"]:
            print(f"Error Code: {diag['error_code']}")
            print(f"Error Message: {diag['error_message']}")
        if diag["troubleshooting_guide"]:
            print("\n[Actionable Troubleshooting Runbook]")
            for step, desc in diag["troubleshooting_guide"].items():
                print(f"  - {step}: {desc}")

    return 0 if diag["driver_connect_success"] else 1


if __name__ == "__main__":
    sys.exit(main())
