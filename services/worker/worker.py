# services/worker/worker.py
# -*- coding: utf-8 -*-
"""
Worker 定时任务：
1) 每天在 PUSH_HOUR_UTC:PUSH_MINUTE 拉“次日赛程”（API-Football）→ 调你自己的 API /scores/top3?model=ensemble → 推送 Telegram
2) 每 5 分钟健康检查 /healthz，异常报警
环境变量见文件末尾注释或 README。
"""

import os
import json
import time
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ---------------------------
# 环境变量（必须/建议）
# ---------------------------
API_BASE = os.getenv("API_BASE", "").rstrip("/") or os.getenv("API_URL", "").rstrip("/")
# 你的统一 API（FastAPI）地址，例如：https://footballmaster.onrender.com
# 兼容两种变量名，任意一个设了就行

API_SHARED_TOKEN = os.getenv("API_SHARED_TOKEN")  # 访问你 API 的鉴权头 x-api-token
MODEL_TO_USE = os.getenv("PREDICT_MODEL", "ensemble")  # 默认 ensemble

# Telegram（强烈建议，便于收消息/告警）
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 每天推送时间（UTC）
PUSH_HOUR_UTC = int(os.getenv("PUSH_HOUR_UTC", "14"))     # 默认 14:00 UTC（台北 22:00）
PUSH_MINUTE = int(os.getenv("PUSH_MINUTE", "0"))

# API-Football
APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY")
APIFOOTBALL_BASE = os.getenv("APIFOOTBALL_BASE", "https://v3.football.api-sports.io").rstrip("/")
APIFOOTBALL_LEAGUE_IDS = [s.strip() for s in os.getenv("APIFOOTBALL_LEAGUE_IDS", "").split(",") if s.strip()]
APIFOOTBALL_SEASON = os.getenv("APIFOOTBALL_SEASON", "")  # 留空=当前赛季
APIFOOTBALL_TIMEZONE = os.getenv("APIFOOTBALL_TIMEZONE", "UTC")

# 其他参数
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "3"))

# ---------------------------
# 基础工具
# ---------------------------

def _http_get(url: str, headers: Dict[str, str] = None, params: Dict[str, Any] = None) -> requests.Response:
    for i in range(RETRY_TIMES):
        r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 429:
            # 速率限制，简单退避
            time.sleep(2 + i)
            continue
        return r
    return r  # 返回最后一次，交由调用方处理

def _http_post_json(url: str, headers: Dict[str, str], data: Dict[str, Any]) -> requests.Response:
    for i in range(RETRY_TIMES):
        r = requests.post(url, headers=headers, json=data, timeout=HTTP_TIMEOUT)
        if r.status_code in (429, 503):
            time.sleep(2 + i)
            continue
        return r
    return r

def _now_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

# ---------------------------
# API-Football: 拉“次日赛程”
# ---------------------------

def _apifootball_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    assert APIFOOTBALL_KEY, "APIFOOTBALL_KEY 未设置"
    url = f"{APIFOOTBALL_BASE}/{path.lstrip('/')}"
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    r = _http_get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()

def _map_fixture(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = item.get("fixture") or {}
    teams = item.get("teams") or {}
    league = item.get("league") or {}

    home = (teams.get("home") or {}).get("name")
    away = (teams.get("away") or {}).get("name")
    kickoff = fixture.get("date")  # 传 timezone=UTC 时返回 UTC ISO8601
    fid = fixture.get("id")
    league_name = league.get("name") or f"League-{league.get('id')}"

    if not (home and away and kickoff and fid):
        return None

    return {
        "home": home,
        "away": away,
        "kickoff": kickoff,
        "league": league_name,
        "match_id": f"af_{fid}",  # 采用 API-Football 的 fixture.id，加前缀避免冲突
    }

def fixtures_next_day() -> List[Dict[str, Any]]:
    tomorrow = (dt.datetime.utcnow() + dt.timedelta(days=1)).date().isoformat()

    fixtures: List[Dict[str, Any]] = []

    # 分两种：限定联赛 vs 不限定（全量）
    if APIFOOTBALL_LEAGUE_IDS:
        for lg in APIFOOTBALL_LEAGUE_IDS:
            page = 1
            while True:
                params = {
                    "date": tomorrow,
                    "league": lg,
                    "timezone": APIFOOTBALL_TIMEZONE,
                    "page": page,
                }
                if APIFOOTBALL_SEASON:
                    params["season"] = APIFOOTBALL_SEASON
                data = _apifootball_get("fixtures", params)
                for it in data.get("response", []):
                    fx = _map_fixture(it)
                    if fx:
                        fixtures.append(fx)
                if page >= (data.get("paging", {}) or {}).get("total", 1):
                    break
                page += 1
    else:
        # 不限定联赛：按日期全量拉（数据量大，配额高的时候使用）
        page = 1
        while True:
            params = {"date": tomorrow, "timezone": APIFOOTBALL_TIMEZONE, "page": page}
            if APIFOOTBALL_SEASON:
                params["season"] = APIFOOTBALL_SEASON
            data = _apifootball_get("fixtures", params)
            for it in data.get("response", []):
                fx = _map_fixture(it)
                if fx:
                    fixtures.append(fx)
            if page >= (data.get("paging", {}) or {}).get("total", 1):
                break
            page += 1

    print(f"[{_now_str()}] [fixtures_next_day] {tomorrow} -> {len(fixtures)} matches")
    return fixtures

# ---------------------------
# 调你自己的 API 做预测 & 组装摘要
# ---------------------------

def call_scores_top3(match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    调你线上 API: POST /scores/top3?model=xxx
    """
    assert API_BASE, "API_BASE 未设置（比如 https://footballmaster.onrender.com）"
    assert API_SHARED_TOKEN, "API_SHARED_TOKEN 未设置（访问你 API 的 x-api-token）"

    url = f"{API_BASE}/scores/top3"
    headers = {
        "x-api-token": API_SHARED_TOKEN,
        "Content-Type": "application/json",
    }
    body = {
        "home": match["home"],
        "away": match["away"],
        "kickoff": match["kickoff"],
        "match_id": match["match_id"],
        "model": MODEL_TO_USE,
    }

    r = _http_post_json(url, headers, body)
    if r.status_code != 200:
        print(f"[{_now_str()}] [scores/top3] HTTP {r.status_code} {r.text[:200]}")
        return None
    try:
        return r.json()
    except Exception:
        print(f"[{_now_str()}] [scores/top3] 非 JSON 响应：{r.text[:200]}")
        return None

def summarize_for_telegram(items: List[Dict[str, Any]]) -> str:
    """
    把多场比赛的预测拼成一条可读的 Telegram 消息。
    """
    lines = []
    lines.append("📌 次日预测（Top3 比分 + 胜/平/负）\n")

    for it in items:
        m = it["match"]
        res = it["result"] or {}
        probs = res.get("probs") or {}
        top3 = res.get("top3_scores") or res.get("top3") or res.get("scores") or []

        def fmt_prob(x):
            try:
                return f"{float(x)*100:.1f}%"
            except Exception:
                return "-"

        lines.append(f"🏟 {m['league']}  {m['home']} vs {m['away']}")
        lines.append(f"🕒 {m['kickoff']} (UTC)")
        lines.append(f"🔢 胜:{fmt_prob(probs.get('home_win'))}  平:{fmt_prob(probs.get('draw'))}  负:{fmt_prob(probs.get('away_win'))}")

        if isinstance(top3, list) and top3 and isinstance(top3[0], dict):
            # 形如 [{"score":"2-1","prob":0.15}, ...]
            top_str = ", ".join([f"{x.get('score')}({fmt_prob(x.get('prob'))})" for x in top3[:3]])
            lines.append(f"🎯 Top3 比分：{top_str}")
        elif isinstance(top3, dict):
            # 形如 {"2-1":0.15,"1-0":0.14,...}
            top_sorted = sorted(top3.items(), key=lambda kv: kv[1], reverse=True)[:3]
            top_str = ", ".join([f"{k}({fmt_prob(v)})" for k, v in top_sorted])
            lines.append(f"🎯 Top3 比分：{top_str}")

        lines.append("")  # 空行分隔

    return "\n".join(lines).strip()

# ---------------------------
# Telegram
# ---------------------------

def telegram_send(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print(f"[{_now_str()}] [telegram] 未配置 TELEGRAM_BOT_TOKEN/CHAT_ID，跳过推送。")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    r = requests.post(url, json=data, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        print(f"[{_now_str()}] [telegram] HTTP {r.status_code} {r.text[:200]}")

# ---------------------------
# 任务：每日推送
# ---------------------------

def job_push_next_day_predictions():
    try:
        fixtures = fixtures_next_day()
        items = []
        for fx in fixtures:
            res = call_scores_top3(fx)
            items.append({"match": fx, "result": res})
            # 简单节流，避免瞬时过快
            time.sleep(0.2)

        msg = summarize_for_telegram(items)
        telegram_send(msg if msg else "明日暂无比赛。")
        print(f"[{_now_str()}] [push] 推送完成。场次={len(items)}")
    except Exception as e:
        err = f"[{_now_str()}] [push] 任务异常：{e}"
        print(err)
        telegram_send("⚠️ 推送任务异常：\n" + str(e))

# ---------------------------
# 任务：健康检查
# ---------------------------

def job_monitor_health():
    try:
        assert API_BASE, "API_BASE 未设置"
        url = f"{API_BASE}/healthz"
        headers = {"x-api-token": API_SHARED_TOKEN} if API_SHARED_TOKEN else {}
        r = _http_get(url, headers=headers)
        ok = (r.status_code == 200 and "ok" in r.text.lower())
        if not ok:
            telegram_send(f"⚠️ 健康检查异常：{r.status_code} {r.text[:200]}")
        print(f"[{_now_str()}] [health] {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[{_now_str()}] [health] 异常：{e}")
        telegram_send(f"⚠️ 健康检查异常：{e}")

# ---------------------------
# 主调度
# ---------------------------

def main():
    print(f"[{_now_str()}] Worker 启动。")
    if not API_BASE:
        print("⚠️ 未设置 API_BASE（例如 https://footballmaster.onrender.com）。")
    if not API_SHARED_TOKEN:
        print("⚠️ 未设置 API_SHARED_TOKEN。")
    if not APIFOOTBALL_KEY:
        print("⚠️ 未设置 APIFOOTBALL_KEY（无法拉真实赛程）。")

    sched = BlockingScheduler(timezone="UTC")

    # 每天固定 UTC 时间推送“次日赛程”
    sched.add_job(
        job_push_next_day_predictions,
        CronTrigger(hour=PUSH_HOUR_UTC, minute=PUSH_MINUTE, timezone="UTC"),
        id="push_next_day",
        replace_existing=True,
    )

    # 每 5 分钟健康检查
    sched.add_job(
        job_monitor_health,
        IntervalTrigger(minutes=5),
        id="monitor_health",
        replace_existing=True,
    )

    print(f"[{_now_str()}] 已注册任务：push@{PUSH_HOUR_UTC:02d}:{PUSH_MINUTE:02d} UTC；health@每5分钟。")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print(f"[{_now_str()}] Worker 退出。")


if __name__ == "__main__":
    main()

# ----------------------------------------------------------------------
# 环境变量速查（Render → API 与 Worker 都建议设同一套）：
#
# 必填：
#   API_BASE                 你的 API 基址（如 https://footballmaster.onrender.com）
#   API_SHARED_TOKEN         访问 API 的令牌（作为请求头 x-api-token）
#   APIFOOTBALL_KEY          API-Football 的 key
#
# 建议：
#   APIFOOTBALL_BASE         https://v3.football.api-sports.io
#   APIFOOTBALL_LEAGUE_IDS   例如：39,78,140,135,61（五大联赛，逗号分隔）
#   APIFOOTBALL_SEASON       留空=当前赛季；实时推送推荐留空；历史导入时可遍历多赛季
#   APIFOOTBALL_TIMEZONE     UTC（全链路统一 UTC）
#   PUSH_HOUR_UTC            每日推送小时（UTC，默认14）
#   PUSH_MINUTE              每日推送分钟（默认0）
#   TELEGRAM_BOT_TOKEN       Telegram 机器人 token（可选但强烈建议）
#   TELEGRAM_CHAT_ID         Telegram 接收人 chat id
#
# 容错/性能：
#   HTTP_TIMEOUT             默认20秒
#   RETRY_TIMES              默认3次
# ----------------------------------------------------------------------
