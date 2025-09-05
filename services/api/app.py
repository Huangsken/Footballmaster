import os, json
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
import httpx
from sqlalchemy import text
from typing import Optional

from common.db import init_db, SessionLocal

# === 环境变量 ===
API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
CALL_MODE = os.getenv("MODEL_CALL_MODE", "local").lower()
ENDPOINT_V5 = os.getenv("MODEL_ENDPOINT_V5", "").strip()
ENDPOINT_TRIAD = os.getenv("MODEL_ENDPOINT_TRIAD", "").strip()

# === 你现有的模型与调度 ===
from models import v5, triad
from cron import start_scheduler

# === 新增：挂载 dpc 与 admin 路由 ===
from api.dpc import router as dpc_router
from api.admin import router as admin_router

app = FastAPI(title="Causal-Football v5.0", version="0.0.1")
init_db()

@app.get("/")
def root():
    return {"message": "FootballMaster API is running!"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.on_event("startup")
def _startup():
    if os.getenv("START_SCHEDULER", "true").lower() == "true":
        start_scheduler()

# 统一鉴权（供本文件内接口使用；dpc/admin 自带各自鉴权）
def _check_auth(token: Optional[str]):
    if not API_TOKEN:
        return
    if not token or token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

# ==== 输入模型 ====
class MatchInput(BaseModel):
    match_id: str
    home: str
    away: str
    # 可扩展：赔率、伤停、Elo 等
    features: dict = Field(default_factory=dict)

class BacktestInput(BaseModel):
    matches: list[MatchInput]

# ==== 工具 ====
async def _call_http(endpoint: str, payload: dict):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(endpoint, json=payload)
        r.raise_for_status()
        return r.json()

def _save_prediction(match_id: str, model: str, payload: dict, result: dict):
    db = SessionLocal()
    try:
        db.execute(
            text("INSERT INTO predictions (match_id, model, payload_json, result_json) "
                 "VALUES (:m,:model,:p,:r)"),
            {"m": match_id, "model": model,
             "p": json.dumps(payload, ensure_ascii=False),
             "r": json.dumps(result, ensure_ascii=False)}
        )
        db.commit()
    finally:
        db.close()

def _select_model(model: str):
    m = (model or "v5").lower()
    if m not in {"v5", "triad", "ensemble"}:
        raise HTTPException(status_code=400, detail="model must be v5|triad|ensemble")
    return m

# ==== 1) 单场预测 ====
@app.post("/predict")
async def predict(
    payload: MatchInput,
    model: str = "v5",
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")
):
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

# ==== 2) 比分 Top3 ====
@app.post("/scores/top3")
async def top3_scores(
    payload: MatchInput,
    model: str = "v5",
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")
):
    _check_auth(x_api_token)
    sel = _select_model(model)
    data = payload.model_dump()

    if CALL_MODE == "http":
        if sel == "ensemble":
            r1 = await _call_http(ENDPOINT_V5, data)
            r2 = await _call_http(ENDPOINT_TRIAD, data)
            res = v5.top3_from_combined(v5.combine_with_triad(r1, r2))
        else:
            r = await _call_http(ENDPOINT_V5 if sel == "v5" else ENDPOINT_TRIAD, data)
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

# ==== 3) 简易回测（胜平负准确率） ====
@app.post("/backtest")
def backtest(
    payload: BacktestInput,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")
):
    _check_auth(x_api_token)
    total, hit = 0, 0
    for m in payload.matches:
        total += 1
        r = v5.predict(m.model_dump())  # 默认用 v5；可按需扩展
        probs = r.get("probs", {})
        pred = max(probs, key=probs.get) if probs else None
        truth = (m.features or {}).get("ft_result")  # H|D|A
        if pred and truth:
            label = {"home_win":"H","draw":"D","away_win":"A"}[pred]
            if label == truth:
                hit += 1
    acc = hit/total if total else 0.0
    return {"total": total, "hit": hit, "acc": round(acc, 4)}

# ==== Assistant（可选） ====
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

@app.post("/assistant")
async def assistant(payload: dict,
                    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _check_auth(x_api_token)
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="assistant not configured")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    body = {"model": OPENAI_MODEL, "input": payload.get("messages", [])}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post("https://api.openai.com/v1/responses", headers=headers, json=body)
        r.raise_for_status()
        return r.json()

# ==== 路由挂载（/dpc/* 与 /admin/*） ====
app.include_router(dpc_router)
app.include_router(admin_router)
