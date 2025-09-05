"""
UID 生成与同批次冲突检测（无数据库依赖）
- 规则：优先使用 provider+id 生成稳定 UID；否则回退到全局散列（不可变）
- 冲突：同批次内检测“同名同生日不同来源ID”→ warn；“同来源ID但不同实体ID”→ block
"""
from __future__ import annotations
from typing import Dict, Iterable, Tuple, Optional
import hashlib
import re

# --- 规范化工具 ---
def _slug(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip().lower())
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s

def normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    return _slug(name)

def hash_short(*parts: str, length: int = 10) -> str:
    h = hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()
    return h[:length]

# --- UID 生成 ---
def make_player_uid(
    provider: Optional[str],
    provider_player_id: Optional[str],
    name: Optional[str],
    birth_date: Optional[str],
) -> str:
    """
    优先：plr_{provider}_{id}
    备用：plr_global_{hash(name,birth_date)}
    """
    if provider and provider_player_id:
        return f"plr_{_slug(provider)}_{_slug(provider_player_id)}"
    # 回退：使用名字+生日做稳定哈希（不可逆、不可变）
    basis = f"{normalize_name(name)}|{(birth_date or '').strip()}"
    return f"plr_global_{hash_short(basis)}"

def make_coach_uid(provider: Optional[str], provider_id: Optional[str], name: Optional[str], birth_date: Optional[str]) -> str:
    if provider and provider_id:
        return f"coach_{_slug(provider)}_{_slug(provider_id)}"
    basis = f"{normalize_name(name)}|{(birth_date or '').strip()}"
    return f"coach_global_{hash_short(basis)}"

def make_ref_uid(provider: Optional[str], provider_id: Optional[str], name: Optional[str], birth_date: Optional[str]) -> str:
    if provider and provider_id:
        return f"ref_{_slug(provider)}_{_slug(provider_id)}"
    basis = f"{normalize_name(name)}|{(birth_date or '').strip()}"
    return f"ref_global_{hash_short(basis)}"

# --- 同批次冲突检测 ---
def detect_conflicts(items: Iterable[dict]) -> Dict[str, str]:
    """
    输入：一批“标准化后的条目”（至少包含 entity_type, entity_id, payload）
    输出：键为 entity_id 的冲突标签：'warn:possible_duplicate' / 'block:provider_id_conflict'
    逻辑：
      - 同来源(provider,provider_id) 出现在多条，但 entity_id 不同 → block
      - 不同来源，但 (name,birth_date) 完全相同 → warn
    """
    by_provider: Dict[Tuple[str, str], str] = {}
    by_name_bd: Dict[Tuple[str, str], str] = {}
    marks: Dict[str, str] = {}

    for it in items:
        t = it.get("entity_type")
        if t not in ("player", "coach", "referee"):
            continue
        eid = it["entity_id"]
        p = (str(it["payload"].get("provider") or "").lower().strip(),
             str(it["payload"].get("provider_id") or it["payload"].get("provider_player_id") or "").strip())
        name = normalize_name(it["payload"].get("name"))
        bd = (it["payload"].get("birth_date") or "").strip()

        # provider 冲突：同一 (provider, id) 不应映射到不同 entity_id
        if p[0] and p[1]:
            if p in by_provider and by_provider[p] != eid:
                marks[eid] = "block:provider_id_conflict"
                marks[by_provider[p]] = "block:provider_id_conflict"
            else:
                by_provider[p] = eid

        # 同名同生日：潜在重复（不同来源 id 也可能是同一人）
        key = (name, bd)
        if name and bd:
            if key in by_name_bd and by_name_bd[key] != eid:
                marks[eid] = "warn:possible_duplicate"
                marks[by_name_bd[key]] = "warn:possible_duplicate"
            else:
                by_name_bd[key] = eid
    return marks
