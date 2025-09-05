"""
factors.py
赛事因子最小骨架：
- 输入：entity_type + payload（比赛/球队/球员上下文）
- 输出：标准化因子(0~1)、解释、以及建议的误差倍率/权重增量
说明：
- 这里只是可运行的最小版，真实数据接入后把 TODO 处换成真实统计即可。
- 约定：数值越大=影响越强（对误差或权重的影响见 mapping）。
"""
from __future__ import annotations
from typing import Dict, Any
from math import exp

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1.0 else x

def _sigmoid(x: float, k: float = 4.0) -> float:
    # 把任意实数压到 0~1（用于粗糙映射）
    return 1.0 / (1.0 + exp(-k * x))

# --- 各因子打分（0~1） ---
def f_schedule_density(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    赛程密度：近 7 天内比赛数 / 平均休息日
    期望：≤3天2赛→偏高
    """
    games_7d = float(payload.get("games_7d", 0))
    avg_rest_day = float(payload.get("avg_rest_day", 4))
    raw = games_7d / 3.0 + (max(0.0, 4.0 - avg_rest_day) / 4.0)
    score = _clamp01(raw * 0.6)  # 简化映射
    return {"name": "schedule_density", "score": score, "explain": f"games_7d={games_7d}, rest={avg_rest_day}d"}

def f_season_phase(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    赛季阶段：开局/中段/尾声/关键战
    """
    phase = (payload.get("phase") or "mid").lower()
    mapping = {"early": 0.35, "mid": 0.5, "late": 0.65, "critical": 0.85}
    score = mapping.get(phase, 0.5)
    return {"name": "season_phase", "score": score, "explain": f"phase={phase}"}

def f_injuries(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    伤停：核心缺阵占权重
    """
    key_out = int(payload.get("key_absent", 0))          # 0/1/2...
    total_out = int(payload.get("total_absent", 0))
    score = _clamp01(0.7 * _sigmoid(key_out) + 0.3 * _clamp01(total_out / 5.0))
    return {"name": "injuries", "score": score, "explain": f"key_out={key_out}, total_out={total_out}"}

def f_referee(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    裁判：红牌率/点球率/临时更换
    """
    red = float(payload.get("red_rate", 0.0))
    pen = float(payload.get("penalty_rate", 0.0))
    swap = bool(payload.get("late_swap", False))
    score = _clamp01(0.5 * red + 0.3 * pen + (0.2 if swap else 0.0))
    return {"name": "referee_volatility", "score": score, "explain": f"red={red}, pen={pen}, swap={swap}"}

def f_media(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    舆论因子：丑闻/转会/高热度
    """
    scandal = bool(payload.get("scandal", False))
    transfer = bool(payload.get("transfer_hot", False))
    hype = float(payload.get("hype_score", 0.0))  # 0~1
    score = _clamp01((0.4 if scandal else 0.0) + (0.3 if transfer else 0.0) + 0.3 * hype)
    return {"name": "media_pressure", "score": score, "explain": f"scandal={scandal}, transfer={transfer}, hype={hype}"}

def f_rivalry(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    宿敌：历史对战强度/仇恨指数
    """
    hist = float(payload.get("derby_strength", 0.0))  # 0~1（历史宿敌强度）
    recent = float(payload.get("recent_tension", 0.0))# 0~1（近年冲突/红黄牌）
    score = _clamp01(0.6 * hist + 0.4 * recent)
    return {"name": "rivalry", "score": score, "explain": f"hist={hist}, recent={recent}"}

def f_social(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    社会因子：疫情/战争/政策（0~3 → 映射到 0~1）
    """
    s = int(payload.get("S", 0))  # 0/1/2/3
    score = _clamp01(s / 3.0)
    return {"name": "social", "score": score, "explain": f"S={s}"}

# --- 映射到“误差倍率/权重加成”的建议（与你的 5.0 公式一致） ---
def impact_mapping(name: str, score: float) -> Dict[str, Any]:
    """
    返回：{ "error_mul": x, "weight_mul": y }
    - error_mul 用于误差调整（<1 降误差，>1 升不确定性）
    - weight_mul 用于样本/因子权重
    """
    if name == "injuries":
        # 核心缺阵：误差 ×0.8（越高越接近 0.8）
        return {"error_mul": 1.0 - 0.2 * score, "weight_mul": 1.0 + 0.5 * score}
    if name == "social":
        # S=3 → 误差 ×0.9
        return {"error_mul": 1.0 - 0.1 * score, "weight_mul": 1.0}
    if name == "referee_volatility":
        # 高红牌率 → 不确定性增加（×1.1 上下浮动）
        return {"error_mul": 1.0 + 0.1 * score, "weight_mul": 1.0}
    if name == "rivalry":
        # 宿敌 → 权重 ×(2~3)（这里线性映射到 1~3）
        return {"error_mul": 1.0, "weight_mul": 1.0 + 2.0 * score}
    if name == "schedule_density":
        return {"error_mul": 1.0 + 0.08 * score, "weight_mul": 1.0 + 0.5 * score}
    if name == "season_phase":
        return {"error_mul": 1.0, "weight_mul": 0.8 + 0.4 * score}
    if name == "media_pressure":
        return {"error_mul": 1.0 + 0.06 * score, "weight_mul": 1.0 + 0.3 * score}
    # 默认
    return {"error_mul": 1.0, "weight_mul": 1.0}

# --- 汇总接口（给 dpc.py 调用） ---
FACTOR_FUNCS = [
    f_schedule_density,
    f_season_phase,
    f_injuries,
    f_referee,
    f_media,
    f_rivalry,
    f_social,
]

def evaluate_factors(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    context 预期包含两侧球队与比赛级字段，这里只取到需要的键即可。
    返回：逐因子分数 + 建议误差倍率/权重 + 总体建议（乘积法）
    """
    items = []
    err_mul = 1.0
    w_mul = 1.0
    for fn in FACTOR_FUNCS:
        r = fn(context or {})
        imp = impact_mapping(r["name"], r["score"])
        r.update(imp)
        items.append(r)
        err_mul *= imp["error_mul"]
        w_mul *= imp["weight_mul"]
    return {
        "items": items,
        "aggregate": {"error_mul": round(err_mul, 4), "weight_mul": round(w_mul, 4)}
    }
