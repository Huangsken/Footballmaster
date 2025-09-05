from fastapi import APIRouter, Header, HTTPException
from app.config import settings
from db.init_db import run_schema

router = APIRouter(prefix="/admin", tags=["admin"])

def _auth_or_403(token: str):
    secret = settings.HMAC_SECRET or ""
    if (token or "").strip() != secret.strip():
        raise HTTPException(status_code=403, detail="forbidden")

@router.post("/init-db")
def init_db(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")):
    _auth_or_403(x_admin_token or "")
    result = run_schema()
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result
