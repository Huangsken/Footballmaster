# services/api/api/admin.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
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
# Test Telegram
# ----------------------
@router.post("/test-tg", summary="Test Telegram Bot")
def test_tg(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    ok, detail = tg_send("✅ test message from /admin/test-tg")
    return {"ok": bool(ok), "detail": detail if not ok else "test message sent"}


# ----------------------
# Check DB connectivity
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
# Init DB schema
# ----------------------
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
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_features (
        id BIGSERIAL PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id   TEXT NOT NULL,
        tool        TEXT NOT NULL,
        feature_key TEXT NOT NULL,
        feature_val JSONB NOT NULL,
        tool_version TEXT NOT NULL,
        source      TEXT,
        confidence  DOUBLE PRECISION,
        computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tool_features_entity ON tool_features(entity_type, entity_id);",
    "CREATE INDEX IF NOT EXISTS idx_tool_features_tool   ON tool_features(tool);",
    "CREATE INDEX IF NOT EXISTS idx_tool_features_key    ON tool_features(feature_key);",
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
    "CREATE INDEX IF NOT EXISTS idx_feature_runs_run ON feature_runs(run_id);",
    """
    CREATE TABLE IF NOT EXISTS experiments (
        id BIGSERIAL PRIMARY KEY,
        exp_id TEXT UNIQUE NOT NULL,
        feature_mask JSONB NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value DOUBLE PRECISION,
        note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]


@router.post("/init-db", summary="Init DB schema")
def init_db_stub(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)
    return {"ok": True, "msg": "schema initialized"}


# ----------------------
# Run Finish 更新
# ----------------------
@router.post("/run-finish", summary="Finish a feature run")
def run_finish(body: dict, x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    """
    更新 feature_runs 的执行结果。
    body 示例:
    {
        "run_id": "run-20250906-001",
        "total": 39,
        "ok": 37,
        "fail": 2,
        "status": "finished",
        "note": "zodiac degrees computed for EPL-2025-01"
    }
    """
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        db.execute(
            text("""
                UPDATE feature_runs
                SET total=:total, ok=:ok, fail=:fail, status=:status, note=:note, finished_at=NOW()
                WHERE run_id=:run_id
            """),
            {
                "run_id": body.get("run_id"),
                "total": body.get("total", 0),
                "ok": body.get("ok", 0),
                "fail": body.get("fail", 0),
                "status": body.get("status", "finished"),
                "note": body.get("note", ""),
            },
        )
        db.commit()
        return {"ok": True, "updated": 1}
    finally:
        db.close()

@router.get("/run-get", summary="List feature run records")
def run_get(
    run_id: Optional[str] = None,
    tool: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    """
    查询 feature_runs 记录。
    - 可选过滤：run_id, tool
    - 分页：limit, offset
    返回字段：id, run_id, tool, total, ok, fail, status, note, started_at, finished_at
    """
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        clauses, params = [], {}
        if run_id:
            clauses.append("run_id = :run_id")
            params["run_id"] = run_id
        if tool:
            clauses.append("tool = :tool")
            params["tool"] = tool
        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        sql = f"""
        SELECT id, run_id, tool, total, ok, fail, status, note,
               started_at, finished_at
        FROM feature_runs
        {where_sql}
        ORDER BY started_at DESC NULLS LAST
        LIMIT :limit OFFSET :offset
        """
        params["limit"] = limit
        params["offset"] = offset

        rows = db.execute(text(sql), params).mappings().all()
        return {
            "ok": True,
            "count": len(rows),
            "items": [dict(r) for r in rows],
        }
    finally:
        db.close()

# ----------------------
# Bulk Feature log（批量写入 tool_features）
# ----------------------
from pydantic import BaseModel, Field
from typing import List
import json
from sqlalchemy import text

# 复用单条的 FeatureLogInput；如你文件里未定义，可取消注释下面的定义
# class FeatureLogInput(BaseModel):
#     entity_type: str
#     entity_id: str
#     tool: str
#     feature_key: str
#     feature_val: dict = Field(default_factory=dict)
#     tool_version: str
#     source: str | None = None
#     confidence: float | None = None
#     computed_at: str | None = None

class FeatureBulkInput(BaseModel):
    items: List[FeatureLogInput] = Field(min_length=1, max_length=1000)
    run_id: str | None = None      # 可选：关联一次批次，自动更新 feature_runs 计数
    dry_run: bool = False          # 只校验不落库（用于先看一眼）

@router.post("/feature-bulk-log", summary="Bulk insert tool_features (with optional run_id aggregation)")
def feature_bulk_log(payload: FeatureBulkInput,
                     x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _auth_or_401(x_api_token)

    rows = []
    for it in payload.items:
        rows.append({
            "entity_type": it.entity_type,
            "entity_id": it.entity_id,
            "tool": it.tool,
            "feature_key": it.feature_key,
            "feature_val": json.dumps(it.feature_val or {}, ensure_ascii=False),
            "tool_version": it.tool_version,
            "source": it.source,
            "confidence": it.confidence,
            "computed_at": it.computed_at,
        })

    if payload.dry_run:
        return {"ok": True, "dry_run": True, "to_insert": len(rows)}

    db = SessionLocal()
    try:
        # 批量 INSERT
        insert_sql = text("""
            INSERT INTO tool_features
                (entity_type, entity_id, tool, feature_key, feature_val,
                 tool_version, source, confidence, computed_at)
            VALUES
                (:entity_type, :entity_id, :tool, :feature_key, :feature_val,
                 :tool_version, :source, :confidence,
                 COALESCE(CAST(:computed_at AS TIMESTAMP), CURRENT_TIMESTAMP))
        """)
        db.execute(insert_sql, rows)

        # 可选：更新 feature_runs 统计
        if payload.run_id:
            agg_sql = text("""
                UPDATE feature_runs
                   SET total = COALESCE(total,0) + :delta,
                       ok    = COALESCE(ok,0)    + :delta
                 WHERE run_id = :rid
            """)
            db.execute(agg_sql, {"delta": len(rows), "rid": payload.run_id})

        db.commit()
        return {"ok": True, "inserted": len(rows), "run_id": payload.run_id}
    finally:
        db.close()
