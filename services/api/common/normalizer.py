"""
normalizer.py
把不同数据源的字段 → 统一映射为 factors.py 所需标准键：
标准键一览（最小集合）：
  games_7d, avg_rest_day, phase, key_absent, total_absent,
  red_rate, penalty_rate, late_swap, scandal, transfer_hot,
  hype_score, derby_strength, recent_tension, S

使用：
  norm_payload = normalize(payload)  # 自动识别 source
"""
from __future__ import annotations
from typing import Dict, Any

def _coerce_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def _coerce_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)

def _truthy(x) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "y", "t", "on"}

# ---------- 1) api-football ----------
def _from_apifootball(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    这里只给最小口径示例：
    - 赛程密度：假设你在上游预聚合了 team 近 7 天 fixtures 数（p["af_games_7d"]）
    - 休息日：p["af_avg_rest_day"]
    - 赛季阶段：依据 p["af_season_round"] 粗糙映射 early/mid/late/critical
    - 裁判：p["af_ref_red_rate"], p["af_ref_pen_rate"]
    """
    phase = "mid"
    rnd = str(p.get("af_season_round") or "").lower()
    if any(k in rnd for k in ["final", "semi", "quarter", "playoff"]):
        phase = "critical"
    elif any(k in rnd for k in ["round_1","matchday_1","md1"]):
        phase = "early"
    elif any(k in rnd for k in ["round_34","md34","matchday_34","last"]):
        phase = "late"

    return {
        "games_7d": _coerce_float(p.get("af_games_7d", 0)),
        "avg_rest_day": _coerce_float(p.get("af_avg_rest_day", 4)),
        "phase": phase,
        "key_absent": _coerce_int(p.get("af_key_absent", 0)),
        "total_absent": _coerce_int(p.get("af_total_absent", 0)),
        "red_rate": _coerce_float(p.get("af_ref_red_rate", 0)),
        "penalty_rate": _coerce_float(p.get("af_ref_pen_rate", 0)),
        "late_swap": _truthy(p.get("af_ref_late_swap", False)),
        "scandal": _truthy(p.get("af_scandal", False)),
        "transfer_hot": _truthy(p.get("af_transfer_hot", False)),
        "hype_score": _coerce_float(p.get("af_hype", 0)),
        "derby_strength": _coerce_float(p.get("af_derby_strength", 0)),
        "recent_tension": _coerce_float(p.get("af_recent_tension", 0)),
        "S": _coerce_int(p.get("af_S", 0)),
    }

# ---------- 2) 爬虫（你自家的 crawler） ----------
def _from_crawler(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    假设爬虫输出采用更口语化命名：
      games_last_7d, rest_avg_days, season_phase, ref_red%, ref_pk%, news_scandal, transfer_heat, hype,
      derby_idx, tension_idx, covid_level(0-3)
    """
    red = p.get("ref_red%")
    pen = p.get("ref_pk%")
    return {
        "games_7d": _coerce_float(p.get("games_last_7d", 0)),
        "avg_rest_day": _coerce_float(p.get("rest_avg_days", 4)),
        "phase": (p.get("season_phase") or "mid").lower(),
        "key_absent": _coerce_int(p.get("key_out", 0)),
        "total_absent": _coerce_int(p.get("total_out", 0)),
        "red_rate": _coerce_float(red, 0) / (100.0 if isinstance(red, (int, float, str)) else 1.0),
        "penalty_rate": _coerce_float(pen, 0) / (100.0 if isinstance(pen, (int, float, str)) else 1.0),
        "late_swap": _truthy(p.get("ref_late_swap", False)),
        "scandal": _truthy(p.get("news_scandal", False)),
        "transfer_hot": _truthy(p.get("transfer_heat", False)),
        "hype_score": _coerce_float(p.get("hype", 0)),
        "derby_strength": _coerce_float(p.get("derby_idx", 0)),
        "recent_tension": _coerce_float(p.get("tension_idx", 0)),
        "S": _coerce_int(p.get("covid_level", 0)),
    }

# ---------- 3) 手工/表单 ----------
def _from_manual(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    手工表单：允许中文/直觉键，如：
      赛程近7天, 平均休息日, 阶段, 关键缺阵, 缺阵总数, 红牌率, 点球率, 临时换裁,
      丑闻, 转会热, 热度, 宿敌强度, 近期紧张, 社会S
    """
    return {
        "games_7d": _coerce_float(p.get("赛程近7天", p.get("games_7d", 0))),
        "avg_rest_day": _coerce_float(p.get("平均休息日", p.get("avg_rest_day", 4))),
        "phase": (p.get("阶段", p.get("phase", "mid")) or "mid").lower(),
        "key_absent": _coerce_int(p.get("关键缺阵", p.get("key_absent", 0))),
        "total_absent": _coerce_int(p.get("缺阵总数", p.get("total_absent", 0))),
        "red_rate": _coerce_float(p.get("红牌率", p.get("red_rate", 0))),
        "penalty_rate": _coerce_float(p.get("点球率", p.get("penalty_rate", 0))),
        "late_swap": _truthy(p.get("临时换裁", p.get("late_swap", False))),
        "scandal": _truthy(p.get("丑闻", p.get("scandal", False))),
        "transfer_hot": _truthy(p.get("转会热", p.get("transfer_hot", False))),
        "hype_score": _coerce_float(p.get("热度", p.get("hype_score", 0))),
        "derby_strength": _coerce_float(p.get("宿敌强度", p.get("derby_strength", 0))),
        "recent_tension": _coerce_float(p.get("近期紧张", p.get("recent_tension", 0))),
        "S": _coerce_int(p.get("社会S", p.get("S", 0))),
    }

# ---------- 入口 ----------
def normalize(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    自动识别来源：
      - payload['source'] == 'apifootball' / 'crawler' / 'manual'
      - 或出现特征键（af_* / games_last_7d / 中文键）时推断
    """
    src = (payload.get("source") or "").lower()
    keys = set(payload.keys())

    if src == "apifootball" or any(k.startswith("af_") for k in keys):
        m = _from_apifootball(payload)
    elif src == "crawler" or "games_last_7d" in keys or "ref_red%" in keys:
        m = _from_crawler(payload)
    elif src == "manual" or any(k in keys for k in ["赛程近7天", "平均休息日", "阶段"]):
        m = _from_manual(payload)
    else:
        # 默认：已是标准键或未知 → 直接取已存在的标准键，缺省补齐
        m = _from_manual(payload)  # 复用容错逻辑

    # 把原始字段也原样保留，方便追踪
    out = dict(payload)
    out.update(m)
    return out
