from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PgConnectResult:
    ok: bool
    message: str


def check_postgres_connectivity(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sslmode: str | None = None,
    connect_timeout_s: int = 3,
) -> PgConnectResult:
    """
    Connectivity check only: open TCP connection + auth, then close.
    No queries are executed.
    """
    try:
        import psycopg  # type: ignore
    except Exception:
        return PgConnectResult(
            ok=False,
            message="psycopg is not installed; cannot validate PostgreSQL connectivity.",
        )

    sslmode_part = f" sslmode={sslmode}" if sslmode else ""
    conninfo = (
        f"host={host} port={port} dbname={database} user={user} password={password}"
        f" connect_timeout={connect_timeout_s}{sslmode_part}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            conn.close()
        return PgConnectResult(ok=True, message="PostgreSQL connection succeeded.")
    except Exception as e:
        return PgConnectResult(ok=False, message=f"PostgreSQL connection failed ({type(e).__name__}).")


def execute_sql(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql: str,
    sslmode: str | None = None,
    connect_timeout_s: int = 5,
) -> tuple[list[str], list[list[Any]], str | None]:
    try:
        import psycopg
    except Exception:
        return [], [], "psycopg is not installed"

    sslmode_part = f" sslmode={sslmode}" if sslmode else ""
    conninfo = (
        f"host={host} port={port} dbname={database} user={user} password={password}"
        f" connect_timeout={connect_timeout_s}{sslmode_part}"
    )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return columns, [list(r) for r in rows], None
                conn.commit()
                return [], [], None
    except Exception as e:
        return [], [], f"Execution failed: {str(e)}"

