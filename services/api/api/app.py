from __future__ import annotations

import os, json
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from common.db import init_db, SessionLocal
try:
    from common.db import engine as db_engine
except Exception:
    from db.connection import engine as db_engine  # type: ignore

from models import v5, triad
from cron import start_scheduler
from api.dpc import router as dpc_router
from api.admin import router as admin_router

API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
CALL_MODE = os.getenv("MODEL_CALL_MODE", "local").lower()
ENDPOINT_V5 = os.getenv("MODEL_ENDPOINT_V5", "").strip()
ENDPOINT_TRIAD = os.getenv("MODEL_ENDPOINT_TRIAD", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

app = FastAPI(title="Causal-Football v5.0", version="0.0.1")

# === CORS 允许跨域 ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # ⚠️ 可改成 ["https://your-frontend.example"]
    allow_methods=["*"],
    allow_headers=["*"],
)

# === 自动建表 ===
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

def init_tables() -> None:
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.on_event("startup")
def _startup():
    init_db()
    init_tables()
    if os.getenv("START_SCHEDULER", "true").lower() == "true":
        start_scheduler()

# === 强制鉴权 ===
def _check_auth(token: str | None):
    if not API_TOKEN:
        raise HTTPException(status_code=500, detail="server misconfigured: API_SHARED_TOKEN missing")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

# === 数据模型 ===
class MatchInput(BaseModel):
    match_id: str
    home: str
    away: str
    features: dict = Field(default_factory=dict)

class BacktestInput(BaseModel):
    matches: list[MatchInput]

class ChatInput(BaseModel):
    messages: list[dict]

# === 工具函数 ===
async def _call_http(endpoint: str, payload: dict):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(endpoint, json=payload)
        r.raise_for_status()
        return r.json()

def _save_prediction(match_id: str, model: str, payload: dict, result: dict):
    db = SessionLocal()
    try:
        db.execute(
            text(
                "INSERT INTO predictions (match_id, model, payload_json, result_json) "
                "VALUES (:m,:model,:p,:r)"
            ),
            {
                "m": match_id,
                "model": model,
                "p": json.dumps(payload, ensure_ascii=False),
                "r": json.dumps(result, ensure_ascii=False),
            },
        )
        db.commit()
    finally:
        db.close()

def _select_model(model: str):
    m = (model or "v5").lower()
    if m not in {"v5", "triad", "ensemble"}:
        raise HTTPException(status_code=400, detail="model must be v5|triad|ensemble")
    return m

# === 接口 ===
@app.post("/predict")
async def predict(payload: MatchInput, model: str = "v5", x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)
    sel  = _select_model(model)
    data = payload.model_dump()

    if CALL_MODE == "http":
        if sel == "v5":
            res = await _call_http(ENDPOINT_V5, data)
        elif sel == "triad":
            res = await _call_http(ENDPOINT_TRIAD, data)
        else:
            r1 = await _call_http(ENDPOINT_V5, data)
            r2 = await _call_http(ENDPOINT_TRIAD, data)
            res = v5.combine_with_triad(r1, r2)
    else:
        if sel == "v5":
            res = v5.predict(data)
        elif sel == "triad":
            res = triad.predict(data)
        else:
            res = v5.combine_with_triad(v5.predict(data), triad.predict(data))

    _save_prediction(payload.match_id, sel, data, res)
    return {"match_id": payload.match_id, "home": payload.home, "away": payload.away, **res}

@app.post("/scores/top3")
async def top3_scores(payload: MatchInput, model: str = "v5", x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)
    sel  = _select_model(model)
    data = payload.model_dump()
    if CALL_MODE == "http":
        if sel == "ensemble":
            r1 = await _call_http(ENDPOINT_V5, data)
            r2 = await _call_http(ENDPOINT_TRIAD, data)
            res = v5.top3_from_combined(v5.combine_with_triad(r1, r2))
        else:
            endpoint = ENDPOINT_V5 if sel == "v5" else ENDPOINT_TRIAD
            r = await _call_http(endpoint, data)
            res = v5.derive_top3_from_result(r)
    else:
        if sel == "v5":
            res = v5.top3_scores(data)
        elif sel == "triad":
            res = triad.top3_scores(data)
        else:
            res = v5.top3_from_combined(
                v5.combine_with_triad(v5.predict(data), triad.predict(data))
            )
    return {"match_id": payload.match_id, "home": payload.home, "away": payload.away, **res}

@app.post("/backtest")
def backtest(payload: BacktestInput, x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)
    total, hit = 0, 0
    for m in payload.matches:
        total += 1
        r = v5.predict(m.model_dump())
        probs = r.get("probs", {})
        pred = max(probs, key=probs.get) if probs else None
        truth = (m.features or {}).get("ft_result")
        if pred and truth:
            label = {"home_win": "H", "draw": "D", "away_win": "A"}[pred]
            if label == truth:
                hit += 1
    acc = hit / total if total else 0.0
    return {"total": total, "hit": hit, "acc": round(acc, 4)}

@app.post("/assistant")
async def assistant(payload: dict, x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="assistant not configured")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    body    = {"model": OPENAI_MODEL, "input": payload.get("messages", [])}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post("https://api.openai.com/v1/responses", headers=headers, json=body)
        r.raise_for_status()
        return r.json()

# === 挂载子路由 ===
app.include_router(dpc_router)
app.include_router(admin_router)

# === 自定义 Swagger：加全局 X-API-Token Header ===
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    for path in openapi_schema.get("paths", {}).values():
        for op in path.values():
            params = op.setdefault("parameters", [])
            params.append({
                "name": "X-API-Token",
                "in": "header",
                "required": True,
                "schema": {"type": "string"},
            })
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
