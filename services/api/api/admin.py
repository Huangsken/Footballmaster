# services/api/api/admin.py
from __future__ import annotations

import os, json
from typing import Optional, List

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from common.notify import tg_send
from common.db import SessionLocal
try:
    from common.db import engine as db_engine
except Exception:
    from db.connection import engine as db_engine  # fallback


router = APIRouter(prefix="/admin", tags=["admin"])
API_TOKEN = os.getenv("API_SHARED_TOKEN", "").strip()


# ----------------------
# Auth helper
# ----------------------
def _auth_or_401(token: Optional[str]):
    if not API_TOKEN:
        return
    if not token or token.strip() != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# ----------------------
# Pydantic models
# ----------------------
class FeatureLogInput(BaseModel):
    entity_type: str
    entity_id: str
    tool: str
    feature_key: str
    feature_val: dict = Field(default_factory=dict)
    tool_version: str
    source: Optional[str] = None
    confidence: Optional[float] = None
    computed_at: Optional[str] = None


class FeatureBulkInput(BaseModel):
    items: List[FeatureLogInput] = Field(min_length=1, max_length=1000)
    run_id: Optional[str] = None
    dry_run: bool = False


class RunStartInput(BaseModel):
    run_id: str
    tool: str
    note: Optional[str] = None


class RunFinishInput(BaseModel):
    run_id: str
    total: int
    ok: int
    fail: int
    status: str = "finished"
    note: Optional[str] = None


# ----------------------
# Telegram 测试
# ----------------------
@router.post("/test-tg", summary="Test Telegram Bot")
def test_tg(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    ok, detail = tg_send("✅ test message from /admin/test-tg")
    return {"ok": bool(ok), "detail": detail if not ok else "test message sent"}


# ----------------------
# DB 连接性检查
# ----------------------
@router.post("/db-check", summary="Check DB connectivity")
def db_check(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        ver = db.execute(text("SELECT version()")).scalar()
        return {"ok": True, "version": ver}
    finally:
        db.close()


# ----------------------
# 初始化 schema
# ----------------------
DDL_STATEMENTS: list[str] = [
    # feature_runs 表增加唯一约束
    """
    CREATE TABLE IF NOT EXISTS feature_runs (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        tool   TEXT NOT NULL,
        total  INT NOT NULL DEFAULT 0,
        ok     INT NOT NULL DEFAULT 0,
        fail   INT NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        note   TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_feature_runs_runid ON feature_runs(run_id);"
]


@router.post("/init-db", summary="Init DB schema")
def init_db_stub(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)
    return {"ok": True, "msg": "schema initialized"}


# ----------------------
# 单条特征写入
# ----------------------
@router.post("/feature-log", summary="Log one tool feature")
def feature_log(body: FeatureLogInput, x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        r = db.execute(
            text("""
                INSERT INTO tool_features (
                    entity_type, entity_id, tool, feature_key, feature_val,
                    tool_version, source, confidence, computed_at
                ) VALUES (
                    :entity_type, :entity_id, :tool, :feature_key, :feature_val::jsonb,
                    :tool_version, :source, :confidence,
                    COALESCE(:computed_at, CURRENT_TIMESTAMP)
                )
                RETURNING id
            """),
            {
                "entity_type": body.entity_type,
                "entity_id": body.entity_id,
                "tool": body.tool,
                "feature_key": body.feature_key,
                "feature_val": json.dumps(body.feature_val, ensure_ascii=False),
                "tool_version": body.tool_version,
                "source": body.source or "manual",
                "confidence": float(body.confidence or 1.0),
                "computed_at": body.computed_at,
            },
        )
        new_id = r.scalar()
        db.commit()
        return {"ok": True, "id": int(new_id) if new_id else None}
    finally:
        db.close()


# ----------------------
# 批量特征写入
# ----------------------
@router.post("/feature-bulk-log", summary="Bulk log tool features")
def feature_bulk_log(body: FeatureBulkInput, x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)

    if body.dry_run:
        return {"ok": True, "dry_run": True, "count": len(body.items)}

    db = SessionLocal()
    inserted = 0
    ids: list[int] = []
    try:
        for item in body.items:
            r = db.execute(
                text("""
                    INSERT INTO tool_features (
                        entity_type, entity_id, tool, feature_key, feature_val,
                        tool_version, source, confidence, computed_at
                    ) VALUES (
                        :entity_type, :entity_id, :tool, :feature_key, :feature_val::jsonb,
                        :tool_version, :source, :confidence,
                        COALESCE(:computed_at, CURRENT_TIMESTAMP)
                    )
                    RETURNING id
                """),
                {
                    "entity_type": item.entity_type,
                    "entity_id": item.entity_id,
                    "tool": item.tool,
                    "feature_key": item.feature_key,
                    "feature_val": json.dumps(item.feature_val, ensure_ascii=False),
                    "tool_version": item.tool_version,
                    "source": item.source or "manual",
                    "confidence": float(item.confidence or 1.0),
                    "computed_at": item.computed_at,
                },
            )
            ids.append(r.scalar())
            inserted += 1

        if body.run_id:
            db.execute(
                text("""
                    UPDATE feature_runs
                    SET ok = ok + :ok, total = total + :total
                    WHERE run_id = :run_id
                """),
                {"ok": inserted, "total": inserted, "run_id": body.run_id},
            )

        db.commit()
        return {"ok": True, "inserted": inserted, "ids": ids, "run_id": body.run_id}
    finally:
        db.close()


# ----------------------
# run-start / run-finish / run-get
# ----------------------
@router.post("/run-start", summary="Start a feature run")
def run_start(body: RunStartInput, x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO feature_runs (run_id, tool, total, ok, fail, status, note)
                VALUES (:run_id, :tool, 0, 0, 0, 'running', :note)
                ON CONFLICT (run_id) DO NOTHING
            """),
            {"run_id": body.run_id, "tool": body.tool, "note": body.note},
        )
        db.commit()
        return {"ok": True, "run_id": body.run_id}
    finally:
        db.close()
