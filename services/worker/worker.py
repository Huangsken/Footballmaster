import os
import time
import requests


def send_telegram(text: str):
    """向 Telegram 发送一条消息"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("❌ TELEGRAM 环境变量没有设置好")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        res = requests.post(url, data={"chat_id": chat_id, "text": text})
        print("📩 Telegram 响应:", res.json())
    except Exception as e:
        print("⚠️ Telegram 推送失败:", e)


def main_loop():
    """主循环：每 60 秒推送一次心跳"""
    send_telegram("✅ Footballmaster 服务启动成功！")

    while True:
        print("⏳ Worker still running... will notify every 5 minutes.")
        send_telegram("⚽ Footballmaster 仍在运行中（测试心跳消息）")
        time.sleep(300)  # 每 300 秒（5 分钟）发一次


if __name__ == "__main__":
    main_loop()
