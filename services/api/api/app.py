# services/api/api/app.py
from __future__ import annotations

import os, json
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

# === 你的项目内模块 ===
from common.db import init_db, SessionLocal
from models import v5, triad
from cron import start_scheduler

# 路由（已在 services/api/api/ 下准备好）
from api.dpc import router as dpc_router
from api.admin import router as admin_router

# === 环境变量 ===
API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
CALL_MODE = os.getenv("MODEL_CALL_MODE", "local").lower()          # local | http
ENDPOINT_V5 = os.getenv("MODEL_ENDPOINT_V5", "").strip()
ENDPOINT_TRIAD = os.getenv("MODEL_ENDPOINT_TRIAD", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

# === 应用 ===
app = FastAPI(title="Causal-Football v5.0", version="0.0.1")


# ------------------------------------------------------------
# 自动建表（首次启动 or 表不存在时创建；存在则忽略）
# ------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dpc_ingest_audit (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    source_id TEXT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT,
    confidence FLOAT,
    signature TEXT,
    status TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dpc_ingest_entity
ON dpc_ingest_audit(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL,
    model TEXT NOT NULL,
    payload_json JSONB,
    result_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_predictions_match_model
ON predictions(match_id, model);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def init_tables():
    db = SessionLocal()
    try:
        db.execute(text(DDL))
        db.commit()
    finally:
        db.close()


# ------------------------------------------------------------
# 健康检查
# ------------------------------------------------------------
@app.get("/healthz")
def healthz():
    # 这里保持简洁：能起服务就返回 ok；如需严格校验可以改为检查必需 env
    return {"status": "ok"}


# ------------------------------------------------------------
# 启动事件：初始化数据库、自动建表、可选开启调度器
# ------------------------------------------------------------
@app.on_event("startup")
def _startup():
    init_db()
    init_tables()
    if os.getenv("START_SCHEDULER", "true").lower() == "true":
        start_scheduler()


# ------------------------------------------------------------
# 鉴权（仅本文件内接口用；/dpc 与 /admin 内部有各自鉴权）
# ------------------------------------------------------------
def _check_auth(token: str | None):
    if not API_TOKEN:
        return
    if not token or token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# ------------------------------------------------------------
# 你的原有数据模型
# ------------------------------------------------------------
class MatchInput(BaseModel):
    match_id: str
    home: str
    away: str
    # 可扩展：赔率、伤停、Elo 等
    features: dict = Field(default_factory=dict)

class BacktestInput(BaseModel):
    matches: list[MatchInput]

class ChatInput(BaseModel):
    messages: list[dict]


# ------------------------------------------------------------
# 工具函数
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# 你的原有接口
# ------------------------------------------------------------
@app.post("/predict")
async def predict(payload: MatchInput, model: str = "v5", x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)
    sel = _select_model(model)
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
    sel = _select_model(model)
    data = payload.model_dump()

    if CALL_MODE == "http":
        if sel == "ensemble":
            r1 = await _call_http(ENDPOINT_V5, data)
            r2 = await _call_http(ENDPOINT_TRIAD, data)
            res = v5.top3_from_combined(v5.combine_with_triad(r1, r2))
        else:
            endpoint = ENDPOINT_V5 if sel == "v5" else ENDPOINT_TRIAD
            r = await _call_http(endpoint, data)
            res = v5.derive_top3_from_result(r)   # 若远端不返回比分分布，这里兜底推导
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
    # 简化示例回测：胜平负准确率
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
    body = {"model": OPENAI_MODEL, "input": payload.get("messages", [])}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post("https://api.openai.com/v1/responses", headers=headers, json=body)
        r.raise_for_status()
        return r.json()


# ------------------------------------------------------------
# 挂载子路由：/dpc/* 与 /admin/*
# ------------------------------------------------------------
app.include_router(dpc_router)
app.include_router(admin_router)
