from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from app.config import settings

_engine: Optional[Engine] = None

def get_engine() -> Engine:
    """惰性创建 Engine；如果 DATABASE_URL 为空，抛出友好错误"""
    global _engine
    if _engine is None:
        if not settings.DATABASE_URL:
            raise RuntimeError("DATABASE_URL 未设置，无法连接数据库。")
        _engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
    return _engine

def exec_sql(sql: str, **params) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params)
