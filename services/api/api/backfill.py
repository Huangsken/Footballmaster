# services/api/api/backfill.py
import os, datetime
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
import httpx
from common.db import SessionLocal
from sqlalchemy import text

router = APIRouter(prefix="/admin", tags=["admin"])

API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")

def _check_auth(token: str | None):
    if not API_TOKEN:
        raise HTTPException(status_code=500, detail="server misconfigured: API_SHARED_TOKEN missing")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

class BackfillBody(BaseModel):
    league: str
    seasons: list[str]

@router.post("/backfill-start")
async def backfill_start(body: BackfillBody, x_api_token: str | None = Header(default=None)):
    _check_auth(x_api_token)

    if not API_FOOTBALL_KEY:
        raise HTTPException(status_code=500, detail="API_FOOTBALL_KEY missing")

    inserted_total = 0
    db = SessionLocal()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for season in body.seasons:
                url = f"https://v3.football.api-sports.io/fixtures?league={body.league}&season={season}"
                r = await client.get(url, headers={"x-apisports-key": API_FOOTBALL_KEY})
                r.raise_for_status()
                data = r.json()
                matches = data.get("response", [])
                for m in matches:
                    db.execute(
                        text(
                            "INSERT INTO matches (match_id, season, league, home, away, date) "
                            "VALUES (:id,:season,:league,:home,:away,:date) "
                            "ON CONFLICT (match_id) DO NOTHING"
                        ),
                        {
                            "id": str(m["fixture"]["id"]),
                            "season": str(season),
                            "league": str(body.league),
                            "home": m["teams"]["home"]["name"],
                            "away": m["teams"]["away"]["name"],
                            "date": m["fixture"]["date"],
                        },
                    )
                db.commit()
                inserted_total += len(matches)
    finally:
        db.close()

    return {
        "ok": True,
        "league": body.league,
        "seasons": body.seasons,
        "inserted_total": inserted_total,
        "finished_at": datetime.datetime.utcnow().isoformat()
    }
