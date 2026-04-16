from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ErrorEnvelope(BaseModel):
    error: str
    detail: Any | None = None


class HealthResponse(BaseModel):
    ok: bool
    service: str = "nl2sql-api"
    version: str = "v1"


class ConnectRequest(BaseModel):
    engine: Literal["postgresql", "mysql", "sqlite", "mssql"] = "postgresql"
    host: str = "localhost"
    port: int | None = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    sslmode: str | None = None

    qdrant_host: str | None = None
    qdrant_port: int | None = None


class ConnectResponse(BaseModel):
    ok: bool
    pg_ok: bool
    qdrant_ok: bool
    message: str
    warnings: list[str] = Field(default_factory=list)


class QueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=4000)
    top_k: int = Field(default=5, ge=1, le=20)
    session_id: str | None = None


class QueryResponse(BaseModel):
    ok: bool
    query: str
    session_id: str
    query_type: Literal["SRD", "MRD", "UNKNOWN"]
    confidence: float | None = None
    resolved_query: str
    schema: dict[str, Any] | None = None
    matched_tables: list[str] = Field(default_factory=list)
    sql: str | None = None
    sql_valid: bool = False
    execution_status: Literal["disabled"] = "disabled"
    can_execute: Literal[False] = False
    warnings: list[str] = Field(default_factory=list)


class SchemaResponse(BaseModel):
    ok: bool
    table_count: int
    schema_data: dict[str, Any] = Field(default_factory=dict)


class ExecuteRequest(BaseModel):
    engine: str = "postgresql"
    host: str = "localhost"
    port: int | None = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    sql: str


class ExecuteResponse(BaseModel):
    ok: bool
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    error: str | None = None

