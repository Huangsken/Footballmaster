# services/api/db/connection.py
from __future__ import annotations
import re
from typing import Any, Dict

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.config import settings

# --- 1) 规范化 URL: postgres:// -> postgresql:// ---
def _normalize_url(url: str | None) -> str:
    if not url:
        return ""
    # 去空白 + 统一前缀
    u = url.strip()
    u = re.sub(r"^postgres://", "postgresql://", u, flags=re.IGNORECASE)
    return u

DB_URL = _normalize_url(settings.DATABASE_URL)

# 小打印帮助你在 Render 日志中确认最终生效的 scheme
print(f"[DB] Using DB URL scheme: {DB_URL.split(':', 1)[0] if DB_URL else 'EMPTY'}")

# --- 2) 创建 Engine / Session ---
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)

# --- 3) 简单执行 SQL 的辅助 ---
def exec_sql(sql: str, **params: Dict[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params)

# --- 4) 首次初始化表（可反复调用，无害） ---
def init_db() -> None:
    with engine.begin() as conn:
        # 预测结果审计表
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS predictions (
            id          BIGSERIAL PRIMARY KEY,
            match_id    TEXT NOT NULL,
            model       TEXT NOT NULL,
            payload_json TEXT,
            result_json  TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # DPC 审计表
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS dpc_ingest_audit (
            id          BIGSERIAL PRIMARY KEY,
            run_id      TEXT,
            source_id   TEXT,
            entity_type TEXT,
            entity_id   TEXT,
            action      TEXT,
            confidence  DOUBLE PRECISION,
            signature   TEXT,
            status      TEXT,
            message     TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
