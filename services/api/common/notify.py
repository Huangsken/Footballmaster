import os
import requests

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def tg_send(text: str) -> bool:
    """
    简单封装的 Telegram 发送函数
    """
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text
    })
    return resp.ok

def notify_summary(title: str, body: str) -> bool:
    """
    对外统一入口
    """
    text = f"📢 {title}\n\n{body}"
    return tg_send(text)
