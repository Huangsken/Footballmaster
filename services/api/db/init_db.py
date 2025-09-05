from pathlib import Path
from app.config import settings
from .connection import exec_sql

SCHEMA_FILE = Path(__file__).parent / "schema.sql"

def run_schema() -> dict:
    if not settings.DATABASE_URL:
        return {"ok": False, "msg": "DATABASE_URL 未设置，跳过初始化。"}
    if not SCHEMA_FILE.exists():
        return {"ok": False, "msg": f"未找到 {SCHEMA_FILE.name}"}
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    exec_sql(sql)
    return {"ok": True, "msg": "schema.sql 已执行"}
