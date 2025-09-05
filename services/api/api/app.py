from fastapi import FastAPI, HTTPException
from app.config import settings

app = FastAPI(title="Causal-Football v5.0", version="0.0.1")

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
    if all(required.values()):
        return {"status": "ok", "env": settings.ENV, "tz": settings.TIMEZONE}
    raise HTTPException(
        status_code=500,
        detail={"status": "env-missing", "fields": [k for k, v in required.items() if not v]},
    )
