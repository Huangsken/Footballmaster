# services/api/api/backfill.py
from __future__ import annotations

import os
import time
import json
import logging
from datetime import date

from typing import Optional, List, Dict, Any

import requests
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from common.db import SessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])  # 会在 app 里挂到 prefix="/admin"

API_TOKEN = os.getenv("API_SHARED_TOKEN", "").strip()

# === APIFOOTBALL（apifootball.com） ===
# 你可以通过环境变量覆盖：
#   APIFOOTBALL_BASE = "https://apiv3.apifootball.com/"
#   API_FOOTBALL_KEY = "<你的Key>"
APIFOOTBALL_BASE = os.getenv("APIFOOTBALL_BASE", "https://apiv3.apifootball.com/")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()

# 英超（APIFOOTBALL 的 league_id 常用 152；如与你账号不一致可改成环境变量）
EPL_LEAGUE_ID = os.getenv("APIFOOTBALL_EPL_ID", "152")


def _auth_or_401(token: Optional[str]):
    if not API_TOKEN:
        return
    if not token or token.strip() != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


class BackfillStartBody(BaseModel):
    league: str = Field(..., description="例如 'EPL'")
    seasons: List[str] = Field(..., description="赛季起始年份列表，例如 ['2015','2016']")
    note: Optional[str] = None


def _season_range(yyyy: str) -> tuple[str, str]:
    """把赛季起始年转为大致日期窗口（8/1 ~ 次年 6/30）"""
    y0 = int(yyyy)
    y1 = y0 + 1
    return f"{y0}-08-01", f"{y1}-06-30"


def _apifootball_get_events(league_id: str, date_from: str, date_to: str) -> list[dict]:
    """拉取一段日期里的比赛列表（尽量容错；请求失败返回空列表）"""
    if not API_FOOTBALL_KEY:
        logger.warning("API_FOOTBALL_KEY missing; skip external fetch.")
        return []

    # apifootball.com V3 常见参数：action=get_events
    # 文档版本众多，下面兼容两种常见写法（有些线路需要 /?action=...，有些是纯 query）：
    url = APIFOOTBALL_BASE.rstrip("/") + "/"
    params = {
        "action": "get_events",
        "APIkey": API_FOOTBALL_KEY,
        "league_id": league_id,
        "from": date_from,
        "to": date_to,
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "error" in data:
            logger.error("apifootball error: %s", data)
            return []
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        logger.exception("apifootball request failed: %s", e)
        return []


def _run_start(db, run_id: str, tool: str, note: Optional[str]):
    db.execute(
        text("""
            INSERT INTO feature_runs (run_id, tool, total, ok, fail, status, note)
            VALUES (:run_id, :tool, 0, 0, 0, 'running', :note)
            ON CONFLICT (run_id) DO UPDATE
            SET tool = EXCLUDED.tool,
                status = 'running',
                note = EXCLUDED.note
            RETURNING id, run_id, status
        """),
        {"run_id": run_id, "tool": tool, "note": note},
    )


def _run_finish(db, run_id: str, total: int, ok: int, fail: int, note: Optional[str]):
    db.execute(
        text("""
            UPDATE feature_runs
            SET total=:total, ok=:ok, fail=:fail,
                status='finished', note=:note, finished_at=CURRENT_TIMESTAMP
            WHERE run_id=:run_id
        """),
        {"run_id": run_id, "total": total, "ok": ok, "fail": fail, "note": note},
    )


def _insert_features(db, items: list[dict]) -> tuple[int, list[int]]:
    """把 items 批量写入 tool_features，返回 (成功条数, 生成的ids)"""
    ok = 0
    ids: list[int] = []
    for it in items:
        try:
            r = db.execute(
                text("""
                    INSERT INTO tool_features
                    (entity_type, entity_id, tool, feature_key, feature_val,
                     tool_version, source, confidence, computed_at)
                    VALUES
                    (:entity_type, :entity_id, :tool, :feature_key, CAST(:feature_val AS JSONB),
                     :tool_version, :source, :confidence, COALESCE(:computed_at, CURRENT_TIMESTAMP))
                    RETURNING id
                """),
                {
                    "entity_type": it["entity_type"],
                    "entity_id": it["entity_id"],
                    "tool": it["tool"],
                    "feature_key": it["feature_key"],
                    "feature_val": json.dumps(it["feature_val"], ensure_ascii=False),
                    "tool_version": it.get("tool_version", "1.0.0"),
                    "source": it.get("source", "apifootball"),
                    "confidence": float(it.get("confidence", 1.0)),
                    "computed_at": it.get("computed_at"),
                },
            )
            ids.append(int(r.scalar()))
            ok += 1
        except Exception as e:
            logger.exception("insert feature failed: %s", e)
            db.rollback()
        # 不在此处 commit，外层统一提交
    return ok, ids


@router.post("/backfill-start", summary="Start historical backfill (EPL, APIFOOTBALL)")
def backfill_start(
    body: BackfillStartBody,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    """
    手动触发历史回填。
    输入示例：
    {
      "league": "EPL",
      "seasons": ["2015","2016"],
      "note": "manual backfill"
    }
    返回：run_id，后续可用 /admin/run-get 查询进度。
    """
    _auth_or_401(x_api_token)

    league = body.league.strip().upper()
    if league not in ("EPL", "ENGLAND", "PREMIER_LEAGUE"):
        raise HTTPException(status_code=400, detail="only EPL supported for now")

    run_id = f"backfill_{date.today().strftime('%Y%m%d')}_{int(time.time())}"
    tool_name = "apifootball_backfill"

    db = SessionLocal()
    total = 0
    ok = 0
    fail = 0

    try:
        # 1) 置 running
        _run_start(db, run_id, tool_name, body.note)
        db.commit()

        # 2) 拉取 + 组装特征
        all_items: list[dict] = []

        for y in body.seasons:
            frm, to = _season_range(y)
            events = _apifootball_get_events(EPL_LEAGUE_ID, frm, to)
            if not isinstance(events, list):
                events = []

            for ev in events:
                match_id = str(ev.get("match_id") or ev.get("match_id_ft") or ev.get("match_id_api") or ev.get("match_id_local") or ev.get("match_id", ""))
                if not match_id:
                    # 尽量兜底一个 id（避免空值）
                    match_id = f"EPL-{y}-{ev.get('match_date','unknown')}-{ev.get('match_hometeam_name','H')}vs{ev.get('match_awayteam_name','A')}"
                # 精简保存一份原始字段（避免太大）
                snapshot = {
                    "date": ev.get("match_date"),
                    "time": ev.get("match_time"),
                    "home": ev.get("match_hometeam_name"),
                    "away": ev.get("match_awayteam_name"),
                    "ft_score": ev.get("match_hometeam_ft_score"),
                    "ft_score_away": ev.get("match_awayteam_ft_score"),
                    "status": ev.get("match_status"),
                    "league_id": ev.get("league_id"),
                    "season_hint": y,
                }
                all_items.append({
                    "entity_type": "match",
                    "entity_id": match_id,
                    "tool": "ingest",
                    "feature_key": "raw_event",
                    "feature_val": snapshot,
                    "tool_version": "1.0.0",
                    "source": "apifootball",
                    "confidence": 1.0,
                })

        total = len(all_items)

        # 3) 批量写库（分批提交，防止事务过大）
        BATCH = 200
        written_ids: list[int] = []
        for i in range(0, len(all_items), BATCH):
            chunk = all_items[i:i + BATCH]
            wk, ids = _insert_features(db, chunk)
            ok += wk
            written_ids.extend(ids)
            db.commit()

        fail = total - ok

        # 4) 结束态
        _run_finish(db, run_id, total, ok, fail, body.note or "backfill finished")
        db.commit()

        return {
            "ok": True,
            "run_id": run_id,
            "total": total,
            "ok_count": ok,
            "fail_count": fail,
            "written_ids": written_ids[:10],  # 回显前 10 个以免太长
        }

    except Exception as e:
        db.rollback()
        logger.exception("backfill failed: %s", e)
        # 失败也尽量把 run 标成 finished，便于观察
        try:
            _run_finish(db, run_id, total, ok, total - ok, f"error: {e}")
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=500, detail="backfill failed")
    finally:
        db.close()
