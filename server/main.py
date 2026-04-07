from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from server.config import get_settings
from server.schemas import (
    ConnectRequest,
    ConnectResponse,
    ErrorEnvelope,
    HealthResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    ExecuteRequest,
    ExecuteResponse,
)
from server.services.engine_service import run_readonly_pipeline
from server.services.db_service import check_postgres_connectivity, execute_sql


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(title="NL2SQL API", version="v1")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins or ["*"],
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.get("/api/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(ok=True)

    @app.post(
        "/api/connect",
        response_model=ConnectResponse,
        responses={400: {"model": ErrorEnvelope}, 500: {"model": ErrorEnvelope}},
    )
    def connect(req: ConnectRequest) -> ConnectResponse:
        settings = get_settings()
        warnings: list[str] = []

        qdrant_host = req.qdrant_host or settings.qdrant_host
        qdrant_port = req.qdrant_port or settings.qdrant_port
        
        qdrant_ok = False
        try:
            import httpx
            url = f"http://{qdrant_host}:{qdrant_port}/collections/{settings.qdrant_collection}"
            res = httpx.get(url, timeout=2.0)
            if res.status_code == 200:
                qdrant_msg = "Qdrant connection succeeded."
                qdrant_ok = True
            else:
                qdrant_msg = f"Qdrant returned status {res.status_code}"
        except Exception as e:
            qdrant_msg = f"Qdrant check failed: {type(e).__name__}"

        pg_ok = False
        pg_message = "PostgreSQL connection check skipped."
        if req.engine != "postgresql":
            warnings.append(
                f"Engine '{req.engine}' not supported yet; PostgreSQL-only connectivity checks are implemented."
            )
        else:
            if not req.database:
                warnings.append("Database name is empty.")
            pg_res = check_postgres_connectivity(
                host=req.host,
                port=req.port or 5432,
                database=req.database,
                user=req.username,
                password=req.password,
                sslmode=req.sslmode,
            )
            pg_ok = pg_res.ok
            pg_message = pg_res.message

        ok = bool(qdrant_ok and (pg_ok if req.engine == "postgresql" else True))
        parts = []
        parts.append(qdrant_msg)
        parts.append(pg_message)

        return ConnectResponse(
            ok=ok,
            pg_ok=pg_ok,
            qdrant_ok=qdrant_ok,
            message=" ".join(parts),
            warnings=warnings,
        )

    @app.post(
        "/api/query",
        response_model=QueryResponse,
        responses={400: {"model": ErrorEnvelope}, 500: {"model": ErrorEnvelope}},
    )
    def query(req: QueryRequest) -> QueryResponse:
        try:
            payload = run_readonly_pipeline(
                query=req.query, top_k=req.top_k, session_id=req.session_id
            )
            return QueryResponse(**payload)
        except ValueError as e:
            raise HTTPException(status_code=400, detail={"error": "bad_request", "detail": str(e)})
        except Exception as e:
            raise HTTPException(
                status_code=500, detail={"error": "internal_error", "detail": str(e)}
            )

    @app.get("/api/schema", response_model=SchemaResponse)
    def schema() -> SchemaResponse:
        import json
        try:
            with open("View_Selection/context.json", "r") as f:
                ctx = json.load(f)
        except Exception:
            ctx = {}
        return SchemaResponse(ok=True, table_count=len(ctx or {}), schema_data=ctx or {})

    @app.post("/api/execute", response_model=ExecuteResponse)
    def api_execute(req: ExecuteRequest) -> ExecuteResponse:
        cols, rows, err = execute_sql(
            host=req.host,
            port=req.port or 5432,
            database=req.database,
            user=req.username,
            password=req.password,
            sql=req.sql,
        )
        if err:
            return ExecuteResponse(ok=False, error=err, columns=[], rows=[])
        return ExecuteResponse(ok=True, columns=cols, rows=rows)

    return app


app = create_app()

