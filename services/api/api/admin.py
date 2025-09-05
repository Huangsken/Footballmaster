# services/api/api/admin.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from sqlalchemy import text

from common.db import SessionLocal
from common.notify import tg_send

# 复用统一的建表逻辑（避免重复 DDL）
from api.schema import init_tables

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
# Init DB schema（手动触发同一份建表逻辑）
# ----------------------
@router.post("/init-db", summary="Init DB schema")
def init_db_endpoint(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    init_tables()
    return {"ok": True, "msg": "schema initialized"}


# ----------------------
# Feature log（将工具特征写死到 tool_features）
# ----------------------
from pydantic import BaseModel, Field
import json
from sqlalchemy import text

class FeatureLogInput(BaseModel):
    entity_type: str           # e.g. "match" | "player" | "team"
    entity_id: str             # e.g. "EPL-2025-01-AAA" | "plr_123"
    tool: str                  # e.g. "nine_tools" | "zodiac_degree"
    feature_key: str           # e.g. "zodiac_degree.sun"
    feature_val: dict = Field(default_factory=dict)  # 任意 JSON（将入 JSONB）
    tool_version: str          # e.g. "0.1.0"
    source: str | None = None  # e.g. "manual" | "api"
    confidence: float | None = None  # 0~1
    computed_at: str | None = None   # 可不填：ISO 时间字符串（不填则默认 CURRENT_TIMESTAMP）

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
