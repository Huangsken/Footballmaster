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
# Pydantic models（特征写入 & 运行记录）
# ----------------------
class FeatureLogInput(BaseModel):
    """单条工具特征写入"""
    entity_type: str
    entity_id: str
    tool: str
    feature_key: str
    feature_val: dict = Field(default_factory=dict)
    tool_version: str
    source: Optional[str] = None
    confidence: Optional[float] = None
    computed_at: Optional[str] = None  # ISO 时间字符串（可选）


class FeatureBulkInput(BaseModel):
    """批量工具特征写入"""
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
# 初始化 schema（可幂等）
# ----------------------
DDL_STATEMENTS: list[str] = [
    # dpc_ingest_audit
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

    # predictions
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

    # users
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,

    # tool_features
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

    # feature_runs
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
    # ✅ 关键：为 run_id 创建唯一索引，支撑 ON CONFLICT (run_id)
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_runs_run_id ON feature_runs(run_id);",

    # experiments（保留）
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
# 单条特征写入
# ----------------------
@router.post("/feature-log", summary="Log one tool feature")
def feature_log(
    body: FeatureLogInput,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
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
        return {"ok": True, "id": int(new_id) if new_id is not None else None}
    finally:
        db.close()


# ----------------------
# 批量特征写入
# ----------------------
@router.post("/feature-bulk-log", summary="Bulk log tool features")
def feature_bulk_log(
    body: FeatureBulkInput,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
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
# 查询特征
# ----------------------
@router.get("/feature-get", summary="Get tool features")
def feature_get(
    entity_type: str,
    entity_id: str,
    tool: Optional[str] = None,
    feature_key: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    _auth_or_401(x_api_token)
    where = ["entity_type = :entity_type", "entity_id = :entity_id"]
    params = {"entity_type": entity_type, "entity_id": entity_id, "limit": limit, "offset": offset}
    if tool:
        where.append("tool = :tool"); params["tool"] = tool
    if feature_key:
        where.append("feature_key = :feature_key"); params["feature_key"] = feature_key

    sql = f"""
        SELECT id, entity_type, entity_id, tool, feature_key, feature_val,
               tool_version, source, confidence, computed_at
        FROM tool_features
        WHERE {" AND ".join(where)}
        ORDER BY id DESC
        LIMIT :limit OFFSET :offset
    """
    db = SessionLocal()
    try:
        rows = db.execute(text(sql), params).mappings().all()
        items = []
        for r in rows:
            fv = r["feature_val"]
            if isinstance(fv, str):
                try:
                    fv = json.loads(fv)
                except Exception:
                    pass
            items.append({**dict(r), "feature_val": fv})
        return {"ok": True, "count": len(items), "items": items}
    finally:
        db.close()


# ----------------------
# 运行记录：开始 / 结束 / 查询
# ----------------------
@router.post("/run-start", summary="Start a feature run")
def run_start(
    body: RunStartInput,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
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


@router.post("/run-finish", summary="Finish a feature run")
def run_finish(
    body: RunFinishInput,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        r = db.execute(
            text("""
                UPDATE feature_runs
                SET total = :total, ok = :ok, fail = :fail,
                    status = :status, note = :note, finished_at = CURRENT_TIMESTAMP
                WHERE run_id = :run_id
            """),
            {
                "run_id": body.run_id,
                "total": body.total,
                "ok": body.ok,
                "fail": body.fail,
                "status": body.status,
                "note": body.note,
            },
        )
        db.commit()
        return {"ok": True, "updated": r.rowcount}
    finally:
        db.close()


@router.get("/run-get", summary="Get feature run")
def run_get(
    run_id: str,
    limit: int = 100,
    offset: int = 0,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    _auth_or_401(x_api_token)
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id, run_id, tool, total, ok, fail, status, note,
                       started_at, finished_at
                FROM feature_runs
                WHERE run_id = :run_id
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
            """),
            {"run_id": run_id, "limit": limit, "offset": offset},
        ).mappings().all()
        return {"ok": True, "count": len(rows), "items": [dict(r) for r in rows]}
    finally:
        db.close()
