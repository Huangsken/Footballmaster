from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import Any, List, Optional
from app.config import settings
import time

router = APIRouter(prefix="/dpc", tags=["dpc"])

# ==== 入库数据契约（最小集） ====

class IngestItem(BaseModel):
    # 元信息
    schema_name: str = Field(..., description="逻辑表/事件名，如 player, match, referee, ingest_raw 等")
    schema_version: str = Field(..., description="schema 版本号，如 1.0.0")
    entity_type: str = Field(..., description="player/team/match/referee/news 等")
    entity_id: str = Field(..., description="实体主键或来源方主键")
    # 数据体
    payload: dict[str, Any] = Field(..., description="具体数据内容（已做标准化或原始数据）")
    # 审计信息（可选）
    run_id: Optional[str] = Field(default=None, description="本次任务/抓取批次 ID")
    source_id: Optional[str] = Field(default=None, description="来源标识（供应商/URL/爬虫名）")
    signature: Optional[str] = Field(default=None, description="去重指纹（可选）")
    confidence: Optional[float] = Field(default=None, description="0~1 置信度（可选）")
    snapshot_ts: Optional[float] = Field(default=None, description="Unix 时间戳（可选）")

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        if not (0.0 <= v <= 1.0):
            raise ValueError("confidence must be in [0,1]")
        return v

    @field_validator("entity_id", "schema_name", "schema_version", "entity_type")
    @classmethod
    def _no_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v.strip()

class IngestBatch(BaseModel):
    items: List[IngestItem] = Field(..., description="批量项目，最多 500 条")
    dry_run: bool = Field(True, description="仅校验不落库（默认 True）")

    @field_validator("items")
    @classmethod
    def _limit(cls, v: List[IngestItem]) -> List[IngestItem]:
        if len(v) == 0:
            raise ValueError("items must not be empty")
        if len(v) > 500:
            raise ValueError("items too many (<=500)")
        return v

# ==== 简易规则检查（示例） ====
def _validate_item(it: IngestItem) -> dict:
    """返回 {status, message}；不做数据库写入"""
    # 必备键检查（payload 不可空）
    if not it.payload:
        return {"status": "block", "message": "payload empty"}

    # 体量与长度限制（防止异常超大数据）
    payload_size = len(str(it.payload))
    if payload_size > 200_000:
        return {"status": "block", "message": "payload too large (>200k chars)"}

    # 建议字段：confidence 给出缺失提示
    if it.confidence is None:
        return {"status": "warn", "message": "confidence missing; set to None"}

    # 正常通过
    return {"status": "accepted", "message": "ok"}

def _auth_or_403(token: Optional[str]):
    secret = (settings.HMAC_SECRET or "").strip()
    if not secret:
        # 若你尚未配置 HMAC_SECRET，则允许通过（便于早期测试）
        return
    if (token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="forbidden")

# ==== 路由 ====

@router.post("/ingest")
def ingest(batch: IngestBatch, x_ingest_token: Optional[str] = Header(default=None, alias="X-Ingest-Token")):
    """
    DPC 入库最小契约：仅做字段校验 + 规则判断，返回逐条状态，不落库。
    生产使用时，可在 status=accepted 的条目上对接真正的入库逻辑。
    """
    _auth_or_403(x_ingest_token)
    now = time.time()

    results = []
    for it in batch.items:
        # 默认时间戳补齐（客户端可不传）
        if it.snapshot_ts is None:
            it.snapshot_ts = now
        res = _validate_item(it)
        results.append({
            "entity_type": it.entity_type,
            "entity_id": it.entity_id,
            "schema": f"{it.schema_name}@{it.schema_version}",
            "status": res["status"],
            "message": res["message"],
        })

    overall = "accepted"
    if any(r["status"] == "block" for r in results):
        overall = "block"
    elif any(r["status"] == "warn" for r in results):
        overall = "warn"

    return {
        "ok": True,
        "overall": overall,
        "count": len(results),
        "results": results,
        "dry_run": batch.dry_run,
    }
