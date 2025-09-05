# services/api/api/schema.py
from __future__ import annotations

try:
    # 标准路径
    from common.db import engine as db_engine
except Exception:  # fallback（与你仓库里的 dpc 保持一致）
    from db.connection import engine as db_engine  # type: ignore


# 所有表定义只维护这一份
DDL_STATEMENTS: list[str] = [
    # ===== dpc_ingest_audit =====
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

    # ===== predictions =====
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

    # ===== users =====
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,

    # ===== tool_features（工具特征写死存证） =====
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

    # ===== feature_runs（工具任务运行记录） =====
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

    # ===== experiments（实验与特征开关） =====
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


def init_tables() -> None:
    """幂等建表：启动或手动触发都可执行，多次执行安全。"""
    with db_engine.begin() as conn:
        for sql in DDL_STATEMENTS:
            conn.exec_driver_sql(sql)
