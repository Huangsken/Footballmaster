# services/api/api/admin.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from sqlalchemy import text

from common.notify import tg_send
from common.db import SessionLocal

router = APIRouter(prefix="/admin", tags=["admin"])
API_TOKEN = os.getenv("API_SHARED_TOKEN", "").strip()

def _auth_or_401(token: Optional[str]):
    if not API_TOKEN:
        return
    if not token or token.strip() != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

@router.post("/test-tg", summary="Test Tg")
def test_tg(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    ok, detail = tg_send("test from admin")
    return {"ok": bool(ok), "detail": detail if not ok else "test message sent"}

@router.post("/db-check", summary="Check DB connectivity")
def db_check(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        ver = db.execute(text("SELECT version()")).scalar()
        return {"ok": True, "version": ver}
    finally:
        db.close()

@router.post("/init-db", summary="Init Db Endpoint")
def init_db_stub(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    return {"ok": True, "msg": "schema.sql executed (stub)"}
