# services/api/api/schema.py
from __future__ import annotations

from sqlalchemy import text
from common.db import SessionLocal, engine as db_engine

# 统一的建表 SQL（幂等）
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

    # predictions（模型预测结果）
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

    # users（预留）
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,

    # tool_features（工具特征库）
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

    # feature_runs（批次运行记录）
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
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_feature_runs_run_id ON feature_runs(run_id);",

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
    """,

    # ✅ matches（backfill 落库目标表）
    """
    CREATE TABLE IF NOT EXISTS matches (
        id BIGSERIAL PRIMARY KEY,
        match_id TEXT NOT NULL,
        season   TEXT NOT NULL,
        league   TEXT NOT NULL,
        home     TEXT NOT NULL,
        away     TEXT NOT NULL,
        date     TIMESTAMP,
        raw_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_matches_match_id ON matches(match_id);",
    "CREATE INDEX IF NOT EXISTS idx_matches_season_league ON matches(season, league);",
    "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);",
]


def init_tables() -> dict:
    """
    幂等建表：应用启动时调用。
    """
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)
    return {"ok": True, "msg": "schema initialized"}
