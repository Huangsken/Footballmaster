# services/api/api/admin.py
from __future__ import annotations

import os, json
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
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
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_features (
        id BIGSERIAL PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id   TEXT NOT NULL,
        tool        TEXT NOT NULL,
        feature_key TEXT NOT NULL,
        feature_val JSONB NOT NULL,
        tool_version TEXT NOT NULL,
        source      TEXT,
        confidence  DOUBLE PRECISION,
        computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tool_features_entity ON tool_features(entity_type, entity_id);",
    "CREATE INDEX IF NOT EXISTS idx_tool_features_tool   ON tool_features(tool);",
    "CREATE INDEX IF NOT EXISTS idx_tool_features_key    ON tool_features(feature_key);",
    """
    CREATE TABLE IF NOT EXISTS feature_runs (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        tool   TEXT NOT NULL,
        total  INT NOT NULL DEFAULT 0,
        ok     INT NOT NULL DEFAULT 0,
        fail   INT NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        note   TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_feature_runs_run ON feature_runs(run_id);",
    """
    CREATE TABLE IF NOT EXISTS experiments (
        id BIGSERIAL PRIMARY KEY,
        exp_id TEXT UNIQUE NOT NULL,
        feature_mask JSONB NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value DOUBLE PRECISION,
        note TEXT,
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


# ----------------------
# Feature log（写入 tool_features）
# ----------------------
class FeatureLogInput(BaseModel):
    entity_type: str
    entity_id: str
    tool: str
    feature_key: str
    feature_val: dict = Field(default_factory=dict)
    tool_version: str
    source: str | None = None
    confidence: float | None = None
    computed_at: str | None = None  # ISO 字符串，可空

@router.post("/feature-log", summary="Write tool feature into tool_features")
def feature_log(payload: FeatureLogInput,
                x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        sql = text("""
            INSERT INTO tool_features
                (entity_type, entity_id, tool, feature_key, feature_val,
                 tool_version, source, confidence, computed_at)
            VALUES
                (:entity_type, :entity_id, :tool, :feature_key, :feature_val,
                 :tool_version, :source, :confidence,
                 COALESCE(CAST(:computed_at AS TIMESTAMP), CURRENT_TIMESTAMP))
            RETURNING id
        """)
        params = {
            "entity_type": payload.entity_type,
            "entity_id":   payload.entity_id,
            "tool":        payload.tool,
            "feature_key": payload.feature_key,
            "feature_val": json.dumps(payload.feature_val, ensure_ascii=False),
            "tool_version": payload.tool_version,
            "source":       payload.source,
            "confidence":   payload.confidence,
            "computed_at":  payload.computed_at,
        }
        new_id = db.execute(sql, params).scalar()
        db.commit()
        return {"ok": True, "id": new_id}
    finally:
        db.close()
