from common.factors import evaluate_factors
from common.importance import score as importance_score
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import Any, List, Optional
from app.config import settings
import time

from db.connection import exec_sql
from common.uid import make_player_uid, make_coach_uid, make_ref_uid, detect_conflicts

router = APIRouter(prefix="/dpc", tags=["dpc"])

# ==== 入库数据契约（最小集） ====

class IngestItem(BaseModel):
    # 元信息
    schema_name: str = Field(..., description="逻辑表/事件名，如 player, match, referee, ingest_raw 等")
    schema_version: str = Field(..., description="schema 版本号，如 1.0.0")
    entity_type: str = Field(..., description="player/team/match/referee/news 等")

    # 允许为空；后续会自动补 UID
    entity_id: str | None = Field(default="", description="实体主键或来源方主键；可留空，后续自动生成")

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

    # 注意：entity_id 不再强制非空
    @field_validator("schema_name", "schema_version", "entity_type")
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

# ==== 简易规则检查 ====
def _validate_item(it: IngestItem) -> dict:
    if not it.payload:
        return {"status": "block", "message": "payload empty"}

    payload_size = len(str(it.payload))
    if payload_size > 200_000:
        return {"status": "block", "message": "payload too large (>200k chars)"}

    if it.confidence is None:
        return {"status": "warn", "message": "confidence missing; set to None"}

    return {"status": "accepted", "message": "ok"}

def _auth_or_403(token: Optional[str]):
    secret = (settings.HMAC_SECRET or "").strip()
    if not secret:
        return
    if (token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="forbidden")

# ==== 路由 ====
@router.post("/ingest")
def ingest(batch: IngestBatch, x_ingest_token: Optional[str] = Header(default=None, alias="X-Ingest-Token")):
    _auth_or_403(x_ingest_token)
    now = time.time()

    # === Step1: 补全/标准化 entity_id ===
    normalized_items: List[IngestItem] = []
    for it in batch.items:
        if it.snapshot_ts is None:
            it.snapshot_ts = now

        eid = (it.entity_id or "").strip()
        if it.entity_type == "player" and not eid.startswith("plr_"):
            it.entity_id = make_player_uid(
                provider=it.payload.get("provider"),
                provider_player_id=it.payload.get("provider_player_id") or it.payload.get("provider_id"),
                name=it.payload.get("name"),
                birth_date=it.payload.get("birth_date"),
            )
        elif it.entity_type == "coach" and not eid.startswith("coach_"):
            it.entity_id = make_coach_uid(
                provider=it.payload.get("provider"),
                provider_id=it.payload.get("provider_id"),
                name=it.payload.get("name"),
                birth_date=it.payload.get("birth_date"),
            )
        elif it.entity_type == "referee" and not eid.startswith("ref_"):
            it.entity_id = make_ref_uid(
                provider=it.payload.get("provider"),
                provider_id=it.payload.get("provider_id"),
                name=it.payload.get("name"),
                birth_date=it.payload.get("birth_date"),
            )
        normalized_items.append(it)

    # === Step2: 校验 + 收集 ===
        results = []
    to_insert = []
    for it in normalized_items:
        res = _validate_item(it)

        # 重要度评分（不依赖数据库）
        imp = importance_score(it.entity_type, it.payload)

        results.append({
            "entity_type": it.entity_type,
            "entity_id": it.entity_id,
            "schema": f"{it.schema_name}@{it.schema_version}",
            "status": res["status"],
            "message": res["message"],
            "importance": imp  # {score, tier, priority}
        })

        # 只有通过、且非 dry-run 的才考虑入库
        if res["status"] == "accepted" and not batch.dry_run:
            to_insert.append(it)

    # === Step3: 批内冲突检测 ===
    marks = detect_conflicts([
        {"entity_type": it.entity_type, "entity_id": it.entity_id, "payload": it.payload}
        for it in normalized_items
    ])
    for r in results:
        if r["entity_id"] in marks:
            tag = marks[r["entity_id"]]
            if tag.startswith("block"):
                r["status"] = "block"
            elif tag.startswith("warn") and r["status"] == "accepted":
                r["status"] = "warn"
            r["message"] = (r["message"] + f" | {tag}") if r["message"] else tag

    # === Step4: 真写库（仅 accepted & 非 dry_run） ===
    inserted = 0
    if to_insert:
        sql = """
        INSERT INTO dpc_ingest_audit
        (run_id, source_id, entity_type, entity_id, action, confidence, signature, status, message)
        VALUES (:run_id, :source_id, :entity_type, :entity_id, 'ingest', :confidence, :signature, :status, :message)
        """
        for it in to_insert:
            params = {
                "run_id": it.run_id or "manual",
                "source_id": it.source_id or "unknown",
                "entity_type": it.entity_type,
                "entity_id": it.entity_id,
                "confidence": it.confidence or 1.0,
                "signature": it.signature or None,
                "status": "accepted",
                "message": "ok",
            }
            exec_sql(sql, **params)
            inserted += 1

    # === Step5: overall 状态汇总 ===
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
        "inserted": inserted,
    }
