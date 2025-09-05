from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import requests, html
from app.config import settings

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _tg_creds_ok() -> bool:
    return bool(_clean(settings.TELEGRAM_BOT_TOKEN)) and bool(_clean(settings.TELEGRAM_CHAT_ID))

def tg_send(text: str, parse_mode: str = None) -> Tuple[bool, str]:
    """
    发送 Telegram 消息，返回 (ok, detail)。
    若凭证缺失/网络异常/400-403 等，给出可读 detail，便于你在响应里看到原因。
    """
    if not _tg_creds_ok():
        return False, "MISSING_CREDENTIALS"
    base = f"https://api.telegram.org/bot{_clean(settings.TELEGRAM_BOT_TOKEN)}/sendMessage"
    payload = {
        "chat_id": _clean(settings.TELEGRAM_CHAT_ID),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(base, json=payload, timeout=10)
        ok = 200 <= resp.status_code < 300
        if ok:
            data = {}
            try:
                data = resp.json()
            except Exception:
                pass
            if data.get("ok") is True:
                return True, "OK"
            else:
                return False, f"OK_HTTP_{resp.status_code}_BUT_BODY:{data}"
        else:
            # 常见：400 Can't parse entities / 400 chat not found / 401 Unauthorized
            return False, f"HTTP_{resp.status_code}:{resp.text[:200]}"
    except Exception as e:
        return False, f"EXCEPTION:{type(e).__name__}"

def _fmt_pct(x: float) -> str:
    try: return f"{x*100:.2f}%"
    except Exception: return "n/a"

def _esc(x: Any) -> str:
    return html.escape(str(x)) if x is not None else ""

def build_digest(overall: str, results: List[Dict[str, Any]], run_id: Optional[str] = None, dry_run: bool = True) -> str:
    n = len(results)
    n_acc = sum(1 for r in results if r.get("status") == "accepted")
    n_warn = sum(1 for r in results if r.get("status") == "warn")
    n_blk = sum(1 for r in results if r.get("status") == "block")

    rep = None
    for r in results:
        if (r.get("importance") or {}).get("priority") == 1:
            rep = r; break
    if rep is None and results:
        rep = next((r for r in results if r.get("status") == "accepted"), results[0])

    drivers_html = ""
    if rep:
        causal = rep.get("causal") or {}
        drivers = causal.get("drivers") or []
        rows = [f"• <b>{_esc(d.get('name'))}</b> (score={_esc(d.get('score'))}) — {_esc(d.get('explain'))}" for d in drivers[:3]]
        if rows: drivers_html = "<br/>".join(rows)

    rid = _esc(run_id or "manual")
    lines = [
        f"<b>Ingest Digest</b>  | run_id=<code>{rid}</code>",
        f"overall=<b>{_esc(overall)}</b> | dry_run={str(dry_run).lower()}",
        f"items={n} | accepted={n_acc} | warn={n_warn} | block={n_blk}",
    ]
    if rep:
        cid = _esc(rep.get("entity_id")); schema = _esc(rep.get("schema"))
        imp = rep.get("importance") or {}; tier = _esc(imp.get("tier", "")); pri = _esc(imp.get("priority", ""))
        causal = rep.get("causal") or {}
        adj = _fmt_pct(float(causal.get("adjusted_error", 0.0)))
        em = causal.get("error_mul", 1.0); wm = causal.get("weight_mul", 1.0)
        lines += [f"sample=<code>{cid}</code> | {schema}",
                  f"importance: tier={tier}, priority={pri}",
                  f"causal: adjusted_error≈<b>{adj}</b> | error_mul={em} | weight_mul={wm}"]
        if drivers_html: lines += ["Top drivers:", drivers_html]
    return "<br/>".join(lines)
