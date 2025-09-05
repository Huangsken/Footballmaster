import requests
from app.config import settings
from typing import Tuple


def tg_send(text: str) -> Tuple[bool, str]:
    """
    发送 Telegram 消息（纯文本，不使用 parse_mode）
    """
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        if data.get("ok"):
            return True, "sent"
        else:
            return False, str(data)
    except Exception as e:
        return False, str(e)


def notify_summary(title: str, body: str) -> Tuple[bool, str]:
    """
    封装的通知入口
    """
    text = f"📢 {title}\n\n{body}"
    return tg_send(text)
