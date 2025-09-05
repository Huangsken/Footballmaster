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
