from __future__ import annotations
from typing import Any, Dict, List, Optional
import requests
import html

from app.config import settings

def _tg_creds_ok() -> bool:
    return bool((settings.TELEGRAM_BOT_TOKEN or "").strip()) and bool((settings.TELEGRAM_CHAT_ID or "").strip())

def tg_send(text: str, parse_mode: str = "HTML") -> bool:
    """发送 Telegram 消息；凭证缺失时返回 False 不报错。"""
    if not _tg_creds_ok():
        return False
    base = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(base, json={
            "chat_id": settings.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }, timeout=10)
        return 200 <= resp.status_code < 300
    except Exception:
        return False

def _fmt_pct(x: float) -> str:
    try:
        return f"{x*100:.2f}%"
    except Exception:
        return "n/a"

def _esc(x: Any) -> str:
    return html.escape(str(x)) if x is not None else ""

def build_digest(
    overall: str,
    results: List[Dict[str, Any]],
    run_id: Optional[str] = None,
    dry_run: bool = True,
) -> str:
    """构建推送摘要（HTML），包含：整体状态、条目统计、Top drivers。"""
    n = len(results)
    n_acc = sum(1 for r in results if r.get("status") == "accepted")
    n_warn = sum(1 for r in results if r.get("status") == "warn")
    n_blk = sum(1 for r in results if r.get("status") == "block")

    # 取一个代表性的条目（优先：priority=1，其次 accepted）
    rep = None
    for r in results:
        imp = (r.get("importance") or {}).get("priority")
        if imp == 1:
            rep = r; break
    if rep is None:
        rep = next((r for r in results if r.get("status") == "accepted"), results[0] if results else None)

    # drivers 摘要
    drivers_html = ""
    if rep:
        causal = rep.get("causal") or {}
        drivers = causal.get("drivers") or []
        rows = []
        for d in drivers[:3]:
            rows.append(f"• <b>{_esc(d.get('name'))}</b> (score={_esc(d.get('score'))}) — {_esc(d.get('explain'))}")
        if rows:
            drivers_html = "<br/>".join(rows)

    rid = _esc(run_id or "manual")
    lines = [
        f"<b>Ingest Digest</b>  | run_id=<code>{rid}</code>",
        f"overall=<b>{_esc(overall)}</b> | dry_run={str(dry_run).lower()}",
        f"items={n} | accepted={n_acc} | warn={n_warn} | block={n_blk}",
    ]
    if rep:
        cid = _esc(rep.get("entity_id"))
        schema = _esc(rep.get("schema"))
        imp = rep.get("importance") or {}
        tier = _esc(imp.get("tier", ""))
        pri = _esc(imp.get("priority", ""))
        causal = rep.get("causal") or {}
        adj = _fmt_pct(float(causal.get("adjusted_error", 0.0)))
        em = causal.get("error_mul", 1.0)
        wm = causal.get("weight_mul", 1.0)
        lines += [
            f"sample=<code>{cid}</code> | {schema}",
            f"importance: tier={tier}, priority={pri}",
            f"causal: adjusted_error≈<b>{adj}</b> | error_mul={em} | weight_mul={wm}",
        ]
        if drivers_html:
            lines += ["Top drivers:", drivers_html]

    return "<br/>".join(lines)
