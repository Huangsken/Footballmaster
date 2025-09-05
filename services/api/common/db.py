# common/db.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session


def _normalize_db_url(url: str) -> str:
    """
    Render 的 Postgres 内外链有时会给出 postgres://
    SQLAlchemy 需要 postgresql://
    """
    if not url:
        raise RuntimeError("DATABASE_URL is empty")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


# ---- 构建 Engine & Session 工厂 ----
_DATABASE_URL = _normalize_db_url(os.getenv("DATABASE_URL", "").strip())

# pool_pre_ping=True 防止长连接失效；future=True 走 2.x 接口
engine: Engine = create_engine(
    _DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    """
    应用启动时调用：
    - 验证数据库连通
    - 可按需设置会话级参数（示例设置时区）
    """
    with engine.connect() as conn:
        # 简单连通性探测
        conn.execute(text("SELECT 1"))
        # 需要的话设置时区（可选）
        # conn.execute(text("SET TIME ZONE 'UTC'"))
        conn.commit()


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    可选的依赖式用法：with get_db() as db: ...
    """
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def exec_sql(sql: str, **params: Any) -> None:
    """
    你在 /dpc/ingest 等处使用的简易执行器：
    exec_sql(\"INSERT ... VALUES (:k)\", k=123)
    """
    db: Session = SessionLocal()
    try:
        db.execute(text(sql), params)
        db.commit()
    finally:
        db.close()
