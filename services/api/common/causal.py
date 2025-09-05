"""
causal.py
把 factors.py 的聚合结果转换为“因果快照”：
- 输入：factors = evaluate_factors(...) 的返回
- 可选：从 payload 中读取一些补充因子（如 odds_deviation 暴冷指数 0~1）
- 输出：p0、误差倍率、调整后误差、权重乘子、主导因子解释
"""
from __future__ import annotations
from typing import Dict, Any, List

def _round(x: float, n: int = 4) -> float:
    try:
        return round(float(x), n)
    except Exception:
        return 0.0

def _get(name: str, items: List[Dict[str, Any]], default=1.0) -> float:
    for it in items:
        if it.get("name") == name:
            return float(it.get("error_mul", 1.0)), float(it.get("weight_mul", 1.0)), float(it.get("score", 0.0))
    return float(default), float(default), 0.0

def _top_drivers(items: List[Dict[str, Any]], p0: float) -> List[Dict[str, Any]]:
    # 以 |error_mul-1| 和 |weight_mul-1| 作为影响强度，取前 3
    scored = []
    for it in items:
        em = float(it.get("error_mul", 1.0))
        wm = float(it.get("weight_mul", 1.0))
        impact = abs(em - 1.0) + 0.5 * abs(wm - 1.0)
        scored.append((impact, it))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for _, it in scored[:3]:
        em = float(it.get("error_mul", 1.0))
        wm = float(it.get("weight_mul", 1.0))
        out.append({
            "name": it.get("name"),
            "score": _round(float(it.get("score", 0.0)), 3),
            "impact_error_delta": _round(p0 * (em - 1.0), 5),
            "impact_weight_mul": _round(wm, 4),
            "explain": it.get("explain", "")
        })
    return out

def causal_snapshot(factors: Dict[str, Any], payload: Dict[str, Any], p0: float = 0.021) -> Dict[str, Any]:
    """
    p0: 基础误差率（默认 2.1%）
    组合逻辑：沿用 factors.aggregate 的乘法结构，再叠加“暴冷指数”等补充项
    """
    items = list(factors.get("items", []))
    agg = factors.get("aggregate", {}) or {}
    err_mul = float(agg.get("error_mul", 1.0))
    w_mul = float(agg.get("weight_mul", 1.0))

    # --- 补充：暴冷指数（odds_deviation 0~1；越大越冷门）
    upset = payload.get("odds_deviation")
    if upset is not None:
        try:
            u = max(0.0, min(1.0, float(upset)))
            err_mul *= (1.0 + 0.08 * u)     # 冷门提升不确定性
            w_mul  *= (1.0 + 0.40 * u)     # 给冷门样本更高权重观察
            items.append({
                "name": "upset_index",
                "score": u,
                "error_mul": 1.0 + 0.08 * u,
                "weight_mul": 1.0 + 0.40 * u,
                "explain": f"odds_deviation={u}"
            })
        except Exception:
            pass

    adj_error = p0 * err_mul

    snapshot = {
        "p0": _round(p0, 5),
        "error_mul": _round(err_mul, 4),
        "weight_mul": _round(w_mul, 4),
        "adjusted_error": _round(adj_error, 5),
        "drivers": _top_drivers(items, p0),
    }
    return snapshot
