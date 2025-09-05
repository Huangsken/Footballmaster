"""
importance.py
最小重要度评分规则（0~1）+ 爬虫优先级映射（1~5）
- 不依赖数据库；仅基于 payload 与 entity_type
- 规则可随时迭代：新增字段、调整权重都集中在这里
"""
from __future__ import annotations
from typing import Dict, Any

# ---- 通用工具 ----
def clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def tier_from_score(s: float) -> str:
    if s >= 0.80: return "A"
    if s >= 0.60: return "B"
    if s >= 0.40: return "C"
    return "D"

def priority_from_score(s: float) -> int:
    # 分数越高，优先级数字越小（1=最高）
    if s >= 0.80: return 1
    if s >= 0.60: return 2
    if s >= 0.40: return 3
    if s >= 0.20: return 4
    return 5

# ---- 各实体的打分器 ----
def score_player(p: Dict[str, Any]) -> float:
    """
    参考字段（有则用，无则忽略）：
      position: "F/M/D/GK"
      starter_prob / starter: 先发概率(0~1) 或 bool
      market_value_m: 市值(百万) 粗略映射
      minutes_rolling: 近N场出场时间(0~100比例)
      jersey_no: 号码（低号/10号/7号略加分）
      key_flag: 是否关键球员（上游或人工标注）
    """
    pos = (p.get("position") or "").upper()
    base_by_pos = {"F": 0.70, "M": 0.60, "D": 0.55, "GK": 0.50}
    s = base_by_pos.get(pos, 0.50)

    # 先发概率/标记
    if isinstance(p.get("starter_prob"), (int, float)):
        s += 0.30 * clamp01(float(p["starter_prob"]))
    elif isinstance(p.get("starter"), bool):
        s += 0.20 if p["starter"] else 0.0

    # 市值（对数/阈值化，这里简单阶梯）
    mv = p.get("market_value_m")
    if isinstance(mv, (int, float)):
        if mv >= 80: s += 0.20
        elif mv >= 40: s += 0.12
        elif mv >= 20: s += 0.07
        elif mv >= 5:  s += 0.03

    # 近况（出场时间比例）
    mr = p.get("minutes_rolling")
    if isinstance(mr, (int, float)):
        s += 0.15 * clamp01(float(mr))

    # 球衣号码：10/7/9/8/11/1 等常见核心号码略加分
    jersey = str(p.get("jersey_no") or "").strip()
    if jersey in {"10","7","9"}: s += 0.06
    elif jersey in {"8","11"}:   s += 0.04
    elif jersey in {"1"}:        s += 0.03

    # 关键标记
    if p.get("key_flag") is True:
        s += 0.10

    return clamp01(s)

def score_coach(p: Dict[str, Any]) -> float:
    """
    参考字段：
      stability: 稳定度(0~1)，在位时间或连胜等推断
      style_impact: 战术影响力(0~1)
      reputation: 名望(0~1)
    """
    s = 0.55
    for k, w in [("stability", 0.25), ("style_impact", 0.25), ("reputation", 0.20)]:
        v = p.get(k)
        if isinstance(v, (int, float)): s += w * clamp01(float(v))
    return clamp01(s)

def score_referee(p: Dict[str, Any]) -> float:
    """
    参考字段：
      red_rate: 红牌率(0~1)     → 影响不确定性
      penalty_rate: 点球率(0~1) → 影响比赛走势
      fifa_badge: 是否国际级   → 名单重要性
    """
    s = 0.45
    rr = p.get("red_rate"); pr = p.get("penalty_rate")
    if isinstance(rr, (int, float)): s += 0.20 * clamp01(float(rr))
    if isinstance(pr, (int, float)): s += 0.15 * clamp01(float(pr))
    if p.get("fifa_badge") is True:  s += 0.10
    return clamp01(s)

def score_jersey(p: Dict[str, Any]) -> float:
    """
    球衣号码条目（若作为单独实体采集）
      popularity: 号码在队内受欢迎程度(0~1)
      legacy: 历史传承权重(0~1) 例如队史10号/7号
    """
    s = 0.30
    for k, w in [("popularity", 0.40), ("legacy", 0.40)]:
        v = p.get(k)
        if isinstance(v, (int, float)): s += w * clamp01(float(v))
    return clamp01(s)

def score(entity_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    et = (entity_type or "").lower()
    if et == "player":
        s = score_player(payload or {})
    elif et == "coach":
        s = score_coach(payload or {})
    elif et == "referee":
        s = score_referee(payload or {})
    elif et in {"jersey", "jersey_no", "kit"}:
        s = score_jersey(payload or {})
    else:
        # 未知实体，给基础分
        s = 0.30
    return {
        "score": s,
        "tier": tier_from_score(s),
        "priority": priority_from_score(s)
    }
