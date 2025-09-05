# services/api/api/admin.py
from fastapi import APIRouter, Header, HTTPException
from app.config import settings
from common.notify import tg_send

router = APIRouter(prefix="/admin", tags=["admin"])

def _auth_or_403(token: str | None):
    secret = (settings.HMAC_SECRET or "").strip()
    if not secret:
        return
    if (token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="forbidden")

@router.post("/test-tg")
def test_tg(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")):
    """
    给 Telegram 发一条测试消息：'test from admin'
    仅用于验证通知链路是否 OK。
    """
    _auth_or_403(x_admin_token)
    ok, detail = tg_send("test from admin")
    return {"ok": ok, "detail": detail}

@router.post("/init-db")
def init_db_endpoint(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")):
    _auth_or_403(x_admin_token)
    # 你原来已有的初始化逻辑（省略），这里保持不变或简单返回
    return {"ok": True, "msg": "schema.sql executed (stub)"}
