import os, time, json, datetime as dt
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import httpx
from common.db import init_db, SessionLocal
from sqlalchemy import text

load_dotenv()
init_db()

API_TOKEN = os.getenv("API_SHARED_TOKEN", "")
API_URL = os.getenv("API_URL", "http://api:8080")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PUSH_HOUR_UTC = int(os.getenv("PUSH_HOUR_UTC", "14"))
PUSH_MINUTE = int(os.getenv("PUSH_MINUTE", "0"))

def send_telegram(text_msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[worker] Telegram not configured; skip.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text_msg}
    try:
        r = httpx.post(url, json=data, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("[worker] telegram error:", e)

def fixtures_next_day():
    # TODO: 替换成真实赛程数据源（此为示例）
    tomorrow = (dt.datetime.utcnow().date() + dt.timedelta(days=1)).isoformat()
    return [
        {"match_id": f"DEMO-{tomorrow}-001", "home": "Alpha", "away": "Beta"},
        {"match_id": f"DEMO-{tomorrow}-002", "home": "Gamma", "away": "Delta"},
    ]

def predict_top3(match, model="ensemble"):
    url = f"{API_URL}/scores/top3?model={model}"
    headers = {"x-api-token": API_TOKEN} if API_TOKEN else {}
    with httpx.Client(timeout=30) as client:
        r = client.post(url, headers=headers, json=match)
        r.raise_for_status()
        return r.json()

def build_summary(results):
    lines = []
    lines.append("【次日比赛预测（Top3比分+概率）】")
    for res in results:
        probs = res.get("probs", {})
        lines.append(f"{res['match_id']} {res['home']} vs {res['away']}")
        lines.append(f"胜/平/负：{probs.get('home_win',0):.2f} / {probs.get('draw',0):.2f} / {probs.get('away_win',0):.2f}")
        for i, item in enumerate(res.get("top3_scores", []), start=1):
            lines.append(f"#{i}  {item['score']}  {item['prob']:.2f}")
        lines.append("——")
    return "\n".join(lines)

def push_next_day_predictions():
    matches = fixtures_next_day()
    results = []
    for m in matches:
        try:
            res = predict_top3(m, model="ensemble")  # 默认用融合
            results.append(res)
        except Exception as e:
            print("[worker] predict error:", e)
    if results:
        msg = build_summary(results)
        print(msg)
        send_telegram(msg)

def monitor_health():
    url = f"{API_URL}/healthz"
    try:
        r = httpx.get(url, timeout=5)
        ok = r.status_code == 200 and r.json().get("status") == "ok"
        if not ok:
            send_telegram("【告警】/healthz 异常")
    except Exception as e:
        send_telegram(f"【告警】服务不可达：{e}")

if __name__ == "__main__":
    print("[worker] starting scheduler...")
    scheduler = BackgroundScheduler(timezone="UTC")
    # 推送任务：每天 UTC 指定时刻
    scheduler.add_job(push_next_day_predictions, "cron", hour=PUSH_HOUR_UTC, minute=PUSH_MINUTE)
    # 监控任务：每 5 分钟健康检查
    scheduler.add_job(monitor_health, "cron", minute="*/5")
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
