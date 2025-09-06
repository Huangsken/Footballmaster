# services/api/api/backfill.py
import os
import datetime
from typing import Optional, Dict, Any

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, field_validator
import httpx
from common.db import SessionLocal
from sqlalchemy import text

router = APIRouter(prefix="/admin", tags=["admin"])

# ---- 简单鉴权 ----
API_TOKEN = os.getenv("API_SHARED_TOKEN", "")

def _check_auth(token: Optional[str]):
    if not API_TOKEN:
        raise HTTPException(status_code=500, detail="server misconfigured: API_SHARED_TOKEN missing")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# ---- 英超等联赛映射（可按需扩展）----
LEAGUE_MAP: Dict[str, int] = {
    # 常用别名
    "EPL": 39,
    "PL": 39,
    "premier_league": 39,
    "premier-league": 39,
    "premier league": 39,
    "英超": 39,
    "39": 39,  # 允许直接传字符串 "39"
}

def _resolve_league_id(league: str) -> int:
    league_norm = str(league).strip().lower()
    if league_norm.isdigit():
        return int(league_norm)
    if league_norm in LEAGUE_MAP:
        return int(LEAGUE_MAP[league_norm])
    # 默认尝试原值数字化，否则报错
    try:
        return int(league)
    except Exception:
        raise HTTPException(status_code=400, detail="league must be a numeric id or a known alias like 'EPL' (39)")


# ---- 请求体 ----
class BackfillBody(BaseModel):
    league: str
    seasons: list[str]

    @field_validator("seasons")
    @classmethod
    def _check_seasons(cls, v: list[str]):
        if not v:
            raise ValueError("seasons must not be empty")
        for s in v:
            if not str(s).isdigit():
                raise ValueError(f"season must be numeric like '2015', got: {s}")
        return v


# ---- 调 API 拉取一页 fixtures ----
async def _fetch_fixtures_page(client: httpx.AsyncClient, api_key: str, league_id: int, season: str, page: int) -> Dict[str, Any]:
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"league": league_id, "season": season, "page": page}
    headers = {"x-apisports-key": api_key}
    r = await client.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


# ---- 写入一页数据 ----
def _insert_matches_page(db, league_id: int, season: str, matches: list[dict]) -> int:
    inserted = 0
    for m in matches:
        fx = m.get("fixture", {}) or {}
        tm = m.get("teams", {}) or {}
        home = (tm.get("home") or {}).get("name")
        away = (tm.get("away") or {}).get("name")
        mid  = fx.get("id")
        date = fx.get("date")
        if not (mid and home and away and date):
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
                "id": str(mid),
                "season": str(season),
                "league": str(league_id),
                "home": home,
                "away": away,
                "date": date,
            },
        )
        inserted += 1
    return inserted


# ---- 入口：启动回填 ----
@router.post("/backfill-start")
async def backfill_start(body: BackfillBody, x_api_token: Optional[str] = Header(default=None)):
    _check_auth(x_api_token)

    # 每次调用时现读，避免热更后读到旧值
    api_key = os.getenv("API_FOOTBALL_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="API_FOOTBALL_KEY missing")

    league_id = _resolve_league_id(body.league)

    total_inserted = 0
    per_season_stats: Dict[str, int] = {}

    db = SessionLocal()
    try:
        async with httpx.AsyncClient() as client:
            for season in body.seasons:
                season_inserted = 0
                page = 1
                while True:
                    data = await _fetch_fixtures_page(client, api_key, league_id, season, page)
                    matches = data.get("response", []) or []
                    if not matches:
                        break
                    season_inserted += _insert_matches_page(db, league_id, season, matches)

                    # 分页信息
                    paging = data.get("paging", {}) or {}
                    cur = int(paging.get("current", page))
                    tot = int(paging.get("total", page))
                    if cur >= tot:
                        break
                    page += 1

                db.commit()
                per_season_stats[season] = season_inserted
                total_inserted += season_inserted
    finally:
        db.close()

    return {
        "ok": True,
        "league_id": league_id,
        "seasons": body.seasons,
        "inserted_by_season": per_season_stats,
        "inserted_total": total_inserted,
        "finished_at": datetime.datetime.utcnow().isoformat(),
    }
