# Demo skeleton for V5 model
import random

def predict(data: dict) -> dict:
    # Placeholder: deterministic-ish based on team names for reproducibility
    seed = hash((data["home"], data["away"])) % (10**6)
    rng = random.Random(seed)
    # Simple probabilities
    p_home = 0.4 + (rng.random()-0.5)*0.1
    p_away = 0.3 + (rng.random()-0.5)*0.1
    p_draw = 1 - p_home - p_away
    probs = {"home_win": round(max(0,min(1,p_home)),4),
             "draw": round(max(0,min(1,p_draw)),4),
             "away_win": round(max(0,min(1,p_away)),4)}
    # Simple scoreline grid
    scores = _scoreline_probs(rng, probs)
    return {"probs": probs, "scores": scores, "version":"v5-demo"}

def top3_scores(data: dict) -> dict:
    res = predict(data)
    return top3_from_combined(res)

def combine_with_triad(r1: dict, r2: dict) -> dict:
    # Weighted average ensemble
    def blend(a,b,key,wa=0.6,wb=0.4):
        return round(wa*a.get(key,0)+wb*b.get(key,0),4)
    probs = {
        "home_win": blend(r1.get("probs",{}), r2.get("probs",{}), "home_win"),
        "draw":     blend(r1.get("probs",{}), r2.get("probs",{}), "draw"),
        "away_win": blend(r1.get("probs",{}), r2.get("probs",{}), "away_win"),
    }
    # Merge scoreline distributions if both provide
    scores = r1.get("scores") or {}
    s2 = r2.get("scores") or {}
    all_keys = set(scores)|set(s2)
    merged = {k: round(0.6*scores.get(k,0)+0.4*s2.get(k,0),4) for k in all_keys}
    return {"probs": probs, "scores": merged, "version":"ensemble-0.1"}

def top3_from_combined(res: dict) -> dict:
    scores = res.get("scores") or {}
    top3 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:3]
    return {"probs": res.get("probs", {}),
            "top3_scores": [{"score": s, "prob": round(p,4)} for s,p in top3],
            "version": res.get("version","")}

def derive_top3_from_result(r: dict) -> dict:
    # if remote returns only W/D/L probs, generate heuristic scores
    return top3_from_combined({"probs": r.get("probs",{}), "scores": r.get("scores") or _scoreline_probs(random.Random(), r.get("probs",{}))})

def _scoreline_probs(rng: random.Random, probs: dict) -> dict:
    # Build a tiny scoreline distribution using Poisson-like heuristic
    # Not accurate—just a scaffold
    base_home = 1.4 + rng.random()
    base_away = 1.1 + rng.random()
    candidates = [(0,0),(1,0),(0,1),(1,1),(2,0),(0,2),(2,1),(1,2),(2,2),(3,1),(1,3),(3,2),(2,3)]
    dist = {}
    for h,a in candidates:
        # Simple weight: favor outcomes aligned with W/D/L probs
        if h>a: w = probs.get("home_win",0.33)* (1+0.1*h)
        elif h<a: w = probs.get("away_win",0.33)* (1+0.1*a)
        else: w = probs.get("draw",0.33)* (1+0.1*h)
        # soften by distance from base
        w /= (1+abs(h-base_home)+abs(a-base_away))
        dist[f"{h}-{a}"] = w
    # normalize
    s = sum(dist.values()) or 1.0
    return {k: round(v/s,4) for k,v in dist.items()}
