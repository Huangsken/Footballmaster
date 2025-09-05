# Demo skeleton for "Triad" (三合一) model
import random

def predict(data: dict) -> dict:
    seed = (hash((data["home"], data["away"])) + 7) % (10**6)
    rng = random.Random(seed)
    # Slightly different bias than v5
    p_home = 0.36 + (rng.random()-0.5)*0.08
    p_away = 0.31 + (rng.random()-0.5)*0.08
    p_draw = 1 - p_home - p_away
    probs = {"home_win": round(max(0,min(1,p_home)),4),
             "draw": round(max(0,min(1,p_draw)),4),
             "away_win": round(max(0,min(1,p_away)),4)}
    scores = _scoreline_probs(rng, probs)
    return {"probs": probs, "scores": scores, "version":"triad-demo"}

def top3_scores(data: dict) -> dict:
    res = predict(data)
    # Return same structure as v5
    scores = res.get("scores") or {}
    top3 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:3]
    return {"probs": res.get("probs", {}),
            "top3_scores": [{"score": s, "prob": round(p,4)} for s,p in top3],
            "version": res.get("version","")}

def _scoreline_probs(rng: random.Random, probs: dict) -> dict:
    base_home = 1.3 + rng.random()
    base_away = 1.0 + rng.random()
    candidates = [(0,0),(1,0),(0,1),(1,1),(2,0),(0,2),(2,1),(1,2),(2,2),(3,1),(1,3)]
    dist = {}
    for h,a in candidates:
        if h>a: w = probs.get("home_win",0.33)* (1+0.12*h)
        elif h<a: w = probs.get("away_win",0.33)* (1+0.12*a)
        else: w = probs.get("draw",0.33)* (1+0.07*h)
        w /= (1+abs(h-base_home)+abs(a-base_away))
        dist[f"{h}-{a}"] = w
    s = sum(dist.values()) or 1.0
    return {k: round(v/s,4) for k,v in dist.items()}
