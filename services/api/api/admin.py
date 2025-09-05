# services/api/api/admin.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from sqlalchemy import text

from common.notify import tg_send
from common.db import SessionLocal
try:
    from common.db import engine as db_engine
except Exception:
    from db.connection import engine as db_engine  # fallback

router = APIRouter(prefix="/admin", tags=["admin"])
API_TOKEN = os.getenv("API_SHARED_TOKEN", "").strip()


# ----------------------
# Auth helper
# ----------------------
def _auth_or_401(token: Optional[str]):
    if not API_TOKEN:
        return
    if not token or token.strip() != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# ----------------------
# Test Telegram
# ----------------------
@router.post("/test-tg", summary="Test Telegram Bot")
def test_tg(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    ok, detail = tg_send("✅ test message from /admin/test-tg")
    return {"ok": bool(ok), "detail": detail if not ok else "test message sent"}


# ----------------------
# Check DB connectivity
# ----------------------
@router.post("/db-check", summary="Check DB connectivity")
def db_check(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        ver = db.execute(text("SELECT version()")).scalar()
        return {"ok": True, "version": ver}
    finally:
        db.close()


# ----------------------
# Init DB schema
# ----------------------
DDL_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS dpc_ingest_audit (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT,
        source_id TEXT,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        action TEXT,
        confidence DOUBLE PRECISION,
        signature TEXT,
        status TEXT,
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_dpc_ingest_entity ON dpc_ingest_audit(entity_type, entity_id);",
    """
    CREATE TABLE IF NOT EXISTS predictions (
        id BIGSERIAL PRIMARY KEY,
        match_id TEXT NOT NULL,
        model TEXT NOT NULL,
        payload_json JSONB,
        result_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_predictions_match_model ON predictions(match_id, model);",
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]


@router.post("/init-db", summary="Init DB schema")
def init_db_stub(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)
    return {"ok": True, "msg": "schema initialized"}
