import requests
import uuid
import datetime

API_BASE = "https://footballmaster.onrender.com/admin"
API_TOKEN = f96bf21ddce541b4a89febdde3fdc634

HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "X-API-Token": API_TOKEN,
}

def new_run_id():
    today = datetime.datetime.now().strftime("%Y%m%d")
    return f"EPL-{today}-BATCH-{uuid.uuid4().hex[:6].upper()}"

def run_batch(matches):
    run_id = new_run_id()
    print(f"开始批次: {run_id}")

    # Step 1: run-start
    resp = requests.post(
        f"{API_BASE}/run-start",
        headers=HEADERS,
        json={"run_id": run_id, "tool": "nine_tools", "note": "batch import test"},
    )
    print("run-start:", resp.json())

    # Step 2: feature-bulk-log
    resp = requests.post(
        f"{API_BASE}/feature-bulk-log",
        headers=HEADERS,
        json={"run_id": run_id, "items": matches},
    )
    print("feature-bulk-log:", resp.json())

    # Step 3: run-finish
    resp = requests.post(
        f"{API_BASE}/run-finish",
        headers=HEADERS,
        json={"run_id": run_id, "total": len(matches), "ok": len(matches), "fail": 0, "status": "finished", "note": "batch write finished"},
    )
    print("run-finish:", resp.json())

    # Step 4: run-get
    resp = requests.get(f"{API_BASE}/run-get", headers=HEADERS, params={"run_id": run_id})
    print("run-get:", resp.json())


if __name__ == "__main__":
    # 示例数据：两场比赛
    matches = [
        {
            "entity_type": "match",
            "entity_id": "EPL-2015-01-AAA",
            "tool": "nine_tools",
            "feature_key": "zodiac_degree.sun",
            "feature_val": {"delta": 111.1, "arsenal": 200.0, "chelsea": 95.0},
            "tool_version": "1.0.0",
            "confidence": 0.95,
        },
        {
            "entity_type": "match",
            "entity_id": "EPL-2015-01-BBB",
            "tool": "nine_tools",
            "feature_key": "zodiac_degree.sun",
            "feature_val": {"delta": 222.2, "arsenal": 188.0, "chelsea": 88.0},
            "tool_version": "1.0.0",
            "confidence": 0.9,
        },
    ]

    run_batch(matches)
