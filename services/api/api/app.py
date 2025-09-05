from fastapi import FastAPI
from app.config import settings

app = FastAPI(title="Causal-Football v5.0", version="0.0.1")

# 健康检查（缺项时返回 degraded，但应用不崩）
@app.get("/healthz")
def healthz():
    required = {
        "API_FOOTBALL_KEY": bool(settings.API_FOOTBALL_KEY),
        "DATABASE_URL": bool(settings.DATABASE_URL),
        "REDIS_URL": bool(settings.REDIS_URL),
        "TELEGRAM_BOT_TOKEN": bool(settings.TELEGRAM_BOT_TOKEN),
        "TELEGRAM_CHAT_ID": bool(settings.TELEGRAM_CHAT_ID),
        "HMAC_SECRET": bool(settings.HMAC_SECRET),
    }
    ok = all(required.values())
    return (
        {"status": "ok", "env": settings.ENV, "tz": settings.TIMEZONE}
        if ok
        else {"status": "degraded", "missing": [k for k, v in required.items() if not v]}
    )

# === 这里挂载管理路由（一定要有这两行） ===
from api.admin import router as admin_router  # noqa: E402
app.include_router(admin_router)
