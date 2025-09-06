from __future__ import annotations
import os, sys, time, argparse, json
import httpx
from sqlalchemy import text
from common.db import SessionLocal

API_KEY = os.getenv(310190775014390ec8832b6e80148e88, "").strip()
API_HOST = "https://v3.football.api-sports.io"

# ============ API 调用 ============

def fetch_matches(league: str, season: int):
    """抓取某赛季所有比赛"""
    headers = {"x-apisports-key": API_KEY}
    url = f"{API_HOST}/fixtures"
    params = {"league": 39 if league.upper()=="EPL" else league, "season": season}
    with httpx.Client(timeout=60) as client:
        r = client.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()

# ============ 数据落库 ============

def save_match(raw: dict, run_id: str):
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO dpc_ingest_audit
                (run_id, source_id, entity_type, entity_id, action, confidence, signature, status, message)
                VALUES (:run_id, :source_id, :etype, :eid, :action, :conf, :sig, :status, :msg)
            """),
            {
                "run_id": run_id,
                "source_id": str(raw.get("fixture", {}).get("id")),
                "entity_type": "match",
                "entity_id": str(raw.get("fixture", {}).get("id")),
                "action": "ingest",
                "conf": 1.0,
                "sig": None,
                "status": "ok",
                "msg": json.dumps(raw, ensure_ascii=False),
            },
        )
        db.commit()
    finally:
        db.close()

# ============ 主逻辑 ============

def run_batch(league: str, start_season: int, end_season: int, run_id: str):
    print(f"▶ 开始批处理: {league} {start_season}–{end_season}, run_id={run_id}")
    for season in range(start_season, end_season+1):
        print(f"  抓取赛季 {season}...")
        try:
            data = fetch_matches(league, season)
            matches = data.get("response", [])
            print(f"   共 {len(matches)} 场")
            for m in matches:
                save_match(m, run_id)
            time.sleep(3)  # 降低 API 压力
        except Exception as e:
            print(f"❌ 赛季 {season} 出错: {e}")
            time.sleep(10)

    print("✅ 全部赛季处理完毕")

# ============ CLI ============

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, default="EPL")
    parser.add_argument("--seasons", type=str, required=True, help="e.g. 2014-2024")
    parser.add_argument("--run-id", type=str, required=True)
    args = parser.parse_args()

    try:
        start, end = [int(x) for x in args.seasons.split("-")]
    except Exception:
        print("❌ seasons 参数错误，应为形如 2014-2024")
        sys.exit(1)

    run_batch(args.league, start, end, args.run_id)
