# services/api/api/app.py
from __future__ import annotations

import os, json
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field
from sqlalchemy import text

from common.db import init_db, SessionLocal
from models import v5, triad
from cron import start_scheduler

# 路由
from api.dpc import router as dpc_router
from api.admin import router as admin_router

# 统一复用 schema 的建表逻辑
from api.schema import init_tables

# === 环境变量 ===
API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
CALL_MODE = os.getenv("MODEL_CALL_MODE", "local").lower()          # local | http
ENDPOINT_V5 = os.getenv("MODEL_ENDPOINT_V5", "").strip()
ENDPOINT_TRIAD = os.getenv("MODEL_ENDPOINT_TRIAD", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

# === FastAPI 应用 ===
app = FastAPI(title="Causal-Football v5.0", version="0.0.1")

# CORS（需要前端时再收紧域名）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 健康检查
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# 启动：连接数据库、自动建表、可选调度器
@app.on_event("startup")
def _startup():
    init_db()
    init_tables()  # 幂等
    if os.getenv("START_SCHEDULER", "true").lower() == "true":
        start_scheduler()

# 简单鉴权（本文件内接口用；/dpc 与 /admin 内部自带鉴权）
def _check_auth(token: str | None):
    if not API_TOKEN:
        # 你希望强制要求带 token，这里没有就视为配置错误
        raise HTTPException(status_code=500, detail="server misconfigured: API_SHARED_TOKEN missing")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

# ==== 数据模型 ====
class MatchInput(BaseModel):
    match_id: str
    home: str
    away: str
    features: dict = Field(default_factory=dict)

class BacktestInput(BaseModel):
    matches: list[MatchInput]

class ChatInput(BaseModel):
    messages: list[dict]

# ==== 工具 ====
async def _call_http(endpoint: str, payload: dict):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(endpoint, json=payload)
        r.raise_for_status()
        return r.json()

def _save_prediction(match_id: str, model: str, payload: dict, result: dict):
    """落库预测结果；如需完全兜底可加 try/except，但你现在已能正常入库。"""
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

# ==== 接口 ====
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

# 挂载子路由
app.include_router(dpc_router)
app.include_router(admin_router)

# 自定义 Swagger：要求全局 X-API-Token
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
