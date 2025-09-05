# services/api/api/admin.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from sqlalchemy import text

from common.notify import tg_send
from common.db import SessionLocal

router = APIRouter(prefix="/admin", tags=["admin"])

# 统一的管理口令（与你 .env / Render Environment 中的 API_SHARED_TOKEN 保持一致）
API_TOKEN = os.getenv("API_SHARED_TOKEN", "").strip()


def _auth_or_401(token: Optional[str]):
    """管理类接口的简单鉴权：若配置了 API_SHARED_TOKEN 则必须携带 X-API-Token"""
    if not API_TOKEN:
        return
    if not token or token.strip() != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


@router.post("/test-tg", summary="Test Tg")
def test_tg(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    """
    发送一条测试消息到 Telegram，验证 TG 配置是否正确。
    需要在环境变量中配置：TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
    """
    _auth_or_401(x_api_token)
    ok, detail = tg_send("test from admin")
    if not ok:
        # 返回 200 但说明原因，便于排错
        return {"ok": False, "detail": detail}
    return {"ok": True, "detail": "test message sent"}


@router.post("/db-check", summary="Check DB connectivity")
def db_check(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    """
    连通性检查：执行 SELECT version() 并返回数据库版本。
    成功则说明 DATABASE_URL 正确、驱动加载成功、网络可达。
    """
    _auth_or_401(x_api_token)

    db = SessionLocal()
    try:
        ver = db.execute(text("SELECT version()")).scalar()  # e.g. 'PostgreSQL 15.6 on ...'
        return {"ok": True, "version": ver}
    finally:
        db.close()


@router.post("/init-db", summary="Init Db Endpoint")
def init_db_stub(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    """
    占位实现：真正的建表工作在 app.py 启动时已经自动完成。
    这个接口仅用于演示/回归。
    """
    _auth_or_401(x_api_token)
    return {"ok": True, "msg": "schema.sql executed (stub)"}
