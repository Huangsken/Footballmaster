# services/api/api/backfill.py
from __future__ import annotations

import os
import datetime
from typing import Optional, Dict, Any

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from common.db import SessionLocal

router = APIRouter(prefix="/admin", tags=["admin"])

# 鉴权：每次请求读取，避免热更新/重启顺序导致的“读到旧值”
def _get_api_token() -> str:
    return os.getenv("API_SHARED_TOKEN", "").strip()

def _check_auth(token: Optional[str]):
    api_token = _get_api_token()
    if not api_token:
        raise HTTPException(status_code=500, detail="server misconfigured: API_SHARED_TOKEN missing")
    if token != api_token:
        raise HTTPException(status_code=401, detail="unauthorized")

# ------- 小工具：读取并校验 API_FOOTBALL_KEY -------
def _get_api_football_key() -> str:
    return os.getenv("API_FOOTBALL_KEY", "").strip()

# 可用名字到 league_id 的映射（先内置英超；后面需要可继续扩）
LEAGUE_ALIASES: Dict[str, int] = {
    "epl": 39,
    "premier_league": 39,
    "english_premier_league": 39,
    "england_premier_league": 39,
    "英超": 39,
}

def _normalize_league(league: str) -> int:
    """
    允许传入 "EPL" / "英超" / 39 等；统一转成 API-Football 的联赛 ID（英超=39）
    """
    s = str(league).strip()
    if s.isdigit():
        return int(s)
    key = s.lower().replace("-", "_").replace(" ", "_")
    return LEAGUE_ALIASES.get(key, 39)  # 默认英超

# ------- Pydantic -------
class BackfillBody(BaseModel):
    league: str | int = Field(description="可以是名字（EPL/英超）或 ID（英超=39）")
    seasons: list[str] = Field(min_items=1, description='例如 ["2015"] 或 ["2015","2016"]')

# ------- 诊断接口：不回传密钥原文，只告诉你是否已加载 -------
@router.get("/env-check", summary="Check important envs are loaded")
def env_check(x_api_token: Optional[str] = Header(default=None, alias="X-API-Token")):
    _check_auth(x_api_token)
    return {
        "ok": True,
        "api_shared_token_loaded": bool(_get_api_token()),
        "api_football_key_loaded": bool(_get_api_football_key()),
    }

# ------- 回填入口 -------
@router.post("/backfill-start", summary="Backfill matches into DB from API-Football")
async def backfill_start(
    body: BackfillBody,
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    _check_auth(x_api_token)

    api_key = _get_api_football_key()
    if not api_key:
        # 用明确信息提示你去 Render→Environment 设置，并确保服务重启
        raise HTTPException(status_code=500, detail="API_FOOTBALL_KEY missing")

    league_id = _normalize_league(body.league)

    inserted_total = 0
    per_season_counts: Dict[str, int] = {}

    db = SessionLocal()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for season in body.seasons:
                # API-Football: fixtures?league=39&season=2015
                url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}"
                r = await client.get(url, headers={"x-apisports-key": api_key})
                r.raise_for_status()
                data: Dict[str, Any] = r.json()

                matches = data.get("response", []) or []
                for m in matches:
                    # 做一些健壮性兜底
                    fx = m.get("fixture") or {}
                    tm = m.get("teams") or {}
                    home_name = ((tm.get("home") or {}).get("name")) or ""
                    away_name = ((tm.get("away") or {}).get("name")) or ""
                    match_id = str(fx.get("id") or "")
                    match_date = fx.get("date") or None

                    if not match_id:  # 跳过异常
                        continue

                    db.execute(
                        text(
                            """
                            INSERT INTO matches (match_id, season, league, home, away, date)
                            VALUES (:id, :season, :league, :home, :away, :date)
                            ON CONFLICT (match_id) DO NOTHING
                            """
                        ),
                        {
                            "id": match_id,
                            "season": str(season),
                            "league": str(league_id),
                            "home": home_name,
                            "away": away_name,
                            "date": match_date,
                        },
                    )

                db.commit()
                per_season_counts[str(season)] = len(matches)
                inserted_total += len(matches)

    finally:
        db.close()

    return {
        "ok": True,
        "league_id": league_id,
        "inserted_total": inserted_total,
        "inserted_by_season": per_season_counts,
        "finished_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
