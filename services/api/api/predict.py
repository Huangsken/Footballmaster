from __future__ import annotations

from typing import Any, Optional, List, Dict
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

from app.config import settings
from common.normalizer import normalize
from common.factors import evaluate_factors
from common.importance import score as importance_score
from common.causal import causal_snapshot

import math

router = APIRouter(prefix="/predict", tags=["predict"])


# --------- 输入契约 ---------
class TeamInput(BaseModel):
    name: str = Field(..., description="球队名")
    rating: Optional[float] = Field(
        default=None,
        description="0~1 之间的实力/评分（可选；不传则按 0.5 处理）"
    )

    @field_validator("rating")
    @classmethod
    def _range(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        if not (0.0 <= v <= 1.0):
            raise ValueError("rating must be in [0,1]")
        return v


class PredictInput(BaseModel):
    match_id: str = Field(..., description="比赛唯一 ID")
    home: TeamInput
    away: TeamInput
    # 可以把 ingest 里的 match/player 聚合后的一些关键字段传进来，
    # 比如赛程密度、伤停、红牌倾向等，命名随意，normalize 会做标准化映射。
    payload: Dict[str, Any] = Field(default_factory=dict)

    # 基线误差率 p0（你的 5.0 公式里默认 2.1%）
    p0: float = Field(0.021, description="baseline error rate")

    # 是否需要返回更详细的 explain
    verbose: bool = Field(default=True)


# --------- 鉴权（与 dpc 一致，可选） ---------
def _auth_or_403(token: Optional[str]):
    secret = (settings.HMAC_SECRET or "").strip()
    if not secret:
        return
    if (token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="forbidden")


# --------- 软最大化：把 rating 映射为对胜概率的基线 ---------
def _softmax_2(a: float, b: float, alpha: float = 3.0) -> tuple[float, float]:
    # a/b ∈ [0,1]；alpha 越大区分越强
    ea = math.exp(alpha * a)
    eb = math.exp(alpha * b)
    sa = ea / (ea + eb)
    sb = eb / (ea + eb)
    return sa, sb


# --------- 路由实现 ---------
@router.post("")
def predict(body: PredictInput, x_ingest_token: Optional[str] = Header(default=None, alias="X-Ingest-Token")):
    _auth_or_403(x_ingest_token)

    # 1) 标准化 payload
    payload = normalize(body.payload or {})

    # 2) 重要度与因子
    #    这里 importance 用于给出优先级（例如是否要推送/报警）
    imp = importance_score("match", payload)
    factors = evaluate_factors(payload)

    # 3) 因果快照（根据你 5.0 乘法聚合思想，得出 adjusted_error、error_mul、weight_mul、drivers 等）
    causal = causal_snapshot(factors, payload, p0=body.p0)

    # 4) 基线胜率（来自两队 rating；未提供就用 0.5）
    h_rating = body.home.rating if body.home.rating is not None else 0.5
    a_rating = body.away.rating if body.away.rating is not None else 0.5
    p_home_base, p_away_base = _softmax_2(h_rating, a_rating, alpha=3.0)

    # 5) 用不确定性调整（粗粒度演示版）：
    #    - adjusted_error 越小，说明更确定；我们把“置信”系数 conf = clip(1 - adjusted_error * K)
    #    - 然后把胜率向 0.5 拉回：p_adj = 0.5 + conf*(p_base - 0.5)
    #    你日后可以换成更严谨的贝叶斯缩放。
    adj_err = float(causal.get("adjusted_error", body.p0))
    K = 8.0                                  # 收缩强度；越大越保守
    conf = max(0.0, min(1.0, 1.0 - K * adj_err))

    def shrink(p: float, c: float) -> float:
        return 0.5 + c * (p - 0.5)

    p_home = shrink(p_home_base, conf)
    p_away = shrink(p_away_base, conf)

    # 简单地给出平局概率（演示）：越不确定，平局越高
    p_draw = max(0.0, 1.0 - (p_home + p_away))
    # 归一化一下
    s = p_home + p_away + p_draw
    p_home, p_draw, p_away = p_home / s, p_draw / s, p_away / s

    # 6) 给出建议（demo 逻辑）
    #    - upset_index: 由 factors 里的 “暴冷因子/赔率偏离”等驱动（如果你在 factors 里已有相应字段）
    #      这里找一个可能的字段名，否则回退为 0.0
    upset = 0.0
    agg = (factors or {}).get("aggregate") or {}
    # 如果 evaluate_factors 输出了 'odds_deviation'，就用它映射为 0~1
    if "odds_deviation" in agg:
        x = float(agg["odds_deviation"])
        upset = max(0.0, min(1.0, (x - 0.1) / 0.9))  # 粗映射
    elif "error_mul" in agg:
        x = float(agg["error_mul"])
        upset = max(0.0, min(1.0, (x - 1.0) / 1.5))  # 1.0~2.5 -> 0~1

    advice = "abstain"
    edge_side = None
    edge_size = abs(p_home - p_away)
    if edge_size >= 0.10 and conf >= 0.5 and upset <= 0.5:
        advice = "lean_home" if p_home > p_away else "lean_away"
        edge_side = "home" if p_home > p_away else "away"

    # 7) 输出
    resp = {
        "ok": True,
        "match_id": body.match_id,
        "home": {"name": body.home.name, "rating": h_rating, "prob": round(p_home, 4)},
        "away": {"name": body.away.name, "rating": a_rating, "prob": round(p_away, 4)},
        "draw_prob": round(p_draw, 4),
        "confidence": round(conf, 3),
        "importance": imp,              # {score, tier, priority}
        "factors": factors,             # {items:[...], aggregate:{...}}
        "causal": causal,               # {adjusted_error, error_mul, weight_mul, drivers:[...]}
        "upset_index": round(upset, 3),
        "advice": advice,
        "edge": {"side": edge_side, "size": round(edge_size, 3)},
    }

    return resp
