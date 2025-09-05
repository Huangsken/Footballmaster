# services/api/cron.py
import os, time, datetime as dt, requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

API_BASE = os.getenv("API_BASE", "").rstrip("/")
API_SHARED_TOKEN = os.getenv("API_SHARED_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PUSH_HOUR_UTC = int(os.getenv("PUSH_HOUR_UTC", "14"))
PUSH_MINUTE = int(os.getenv("PUSH_MINUTE", "0"))

APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY")
APIFOOTBALL_BASE = os.getenv("APIFOOTBALL_BASE", "https://v3.football.api-sports.io").rstrip("/")
APIFOOTBALL_LEAGUE_IDS = [s.strip() for s in os.getenv("APIFOOTBALL_LEAGUE_IDS", "").split(",") if s.strip()]
APIFOOTBALL_SEASON = os.getenv("APIFOOTBALL_SEASON", "")
APIFOOTBALL_TIMEZONE = os.getenv("APIFOOTBALL_TIMEZONE", "UTC")

MODEL_TO_USE = os.getenv("PREDICT_MODEL", "ensemble")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

def _now(): return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def tg_send(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): 
        print(f"[{_now()}] [tg] skipped (no token/chat)")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=HTTP_TIMEOUT)
    print(f"[{_now()}] [tg] {r.status_code} {r.text[:120]}")

def _apifootball(path, params):
    assert APIFOOTBALL_KEY, "APIFOOTBALL_KEY not set"
    url = f"{APIFOOTBALL_BASE}/{path.lstrip('/')}"
    r = requests.get(url, params=params, headers={"x-apisports-key": APIFOOTBALL_KEY}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _map_fixture(item):
    fx, teams, league = item.get("fixture") or {}, item.get("teams") or {}, item.get("league") or {}
    home = (teams.get("home") or {}).get("name"); away = (teams.get("away") or {}).get("name")
    kickoff = fx.get("date"); fid = fx.get("id"); lname = league.get("name") or f"L-{league.get('id')}"
    if not (home and away and kickoff and fid): return None
    return {"home": home, "away": away, "kickoff": kickoff, "match_id": f"af_{fid}", "league": lname}

def fixtures_next_day():
    tomorrow = (dt.datetime.utcnow() + dt.timedelta(days=1)).date().isoformat()
    fixtures = []
    def pull_one(params):
        page = 1
        while True:
            p = dict(params, page=page, timezone=APIFOOTBALL_TIMEZONE)
            if APIFOOTBALL_SEASON: p["season"] = APIFOOTBALL_SEASON
            data = _apifootball("fixtures", p)
            for it in data.get("response", []):
                m = _map_fixture(it)
                if m: fixtures.append(m)
            if page >= (data.get("paging", {}) or {}).get("total", 1): break
            page += 1
    if APIFOOTBALL_LEAGUE_IDS:
        for lg in APIFOOTBALL_LEAGUE_IDS: pull_one({"date": tomorrow, "league": lg})
    else:
        pull_one({"date": tomorrow})
    print(f"[{_now()}] [fixtures] {tomorrow} -> {len(fixtures)}")
    return fixtures

def call_scores_top3(match):
    assert API_BASE and API_SHARED_TOKEN
    url = f"{API_BASE}/scores/top3"
    r = requests.post(url, json={
        "home": match["home"], "away": match["away"], "kickoff": match["kickoff"],
        "match_id": match["match_id"], "model": MODEL_TO_USE
    }, headers={"x-api-token": API_SHARED_TOKEN, "Content-Type":"application/json"}, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        print(f"[{_now()}] [scores] {r.status_code} {r.text[:120]}")
        return None
    return r.json()

def summarize(items):
    lines = ["📌 次日预测（Top3比分+胜/平/负）\n"]
    for it in items:
        m, res = it["match"], it["result"] or {}
        probs = res.get("probs") or {}
        top = res.get("top3_scores") or res.get("scores") or {}
        def pct(x): 
            try: return f"{float(x)*100:.1f}%"
            except: return "-"
        lines.append(f"🏟 {m['league']}  {m['home']} vs {m['away']}")
        lines.append(f"🕒 {m['kickoff']} (UTC)")
        lines.append(f"🔢 胜:{pct(probs.get('home_win'))} 平:{pct(probs.get('draw'))} 负:{pct(probs.get('away_win'))}")
        if isinstance(top, dict):
            top3 = ", ".join([f"{k}({pct(v)})" for k,v in sorted(top.items(), key=lambda kv: kv[1], reverse=True)[:3]])
            lines.append(f"🎯 Top3：{top3}")
        lines.append("")
    return "\n".join(lines).strip()

def job_push_next_day():
    try:
        data = []
        for fx in fixtures_next_day():
            data.append({"match": fx, "result": call_scores_top3(fx)})
            time.sleep(0.2)
        msg = summarize(data) if data else "明日暂无比赛。"
        tg_send(msg)
        print(f"[{_now()}] [push] done, matches={len(data)}")
    except Exception as e:
        tg_send(f"⚠️ 推送任务异常：{e}")

def job_health():
    try:
        url = f"{API_BASE}/healthz"; hdr = {"x-api-token": API_SHARED_TOKEN} if API_SHARED_TOKEN else {}
        r = requests.get(url, headers=hdr, timeout=HTTP_TIMEOUT)
        ok = (r.status_code == 200 and "ok" in r.text.lower())
        if not ok: tg_send(f"⚠️ 健康异常：{r.status_code} {r.text[:100]}")
        print(f"[{_now()}] [health] {r.status_code} {r.text[:60]}")
    except Exception as e:
        tg_send(f"⚠️ 健康异常：{e}")

_sched = None
def start_scheduler():
    global _sched
    if _sched: return _sched
    _sched = BackgroundScheduler(timezone="UTC")
    _sched.add_job(job_push_next_day, CronTrigger(hour=PUSH_HOUR_UTC, minute=PUSH_MINUTE, timezone="UTC"),
                   id="push_next_day", replace_existing=True)
    _sched.add_job(job_health, IntervalTrigger(minutes=5), id="health", replace_existing=True)
    _sched.start()
    tg_send("✅ Cron 已启动：每日推送 & 健康检查")
    print(f"[{_now()}] scheduler started @ {PUSH_HOUR_UTC:02d}:{PUSH_MINUTE:02d} UTC")
    return _sched
