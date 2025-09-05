# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先装依赖
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 再拷贝代码（注意路径按你仓库结构）
# API 与调度
COPY services/api/app.py /app/app.py
COPY services/api/cron.py /app/cron.py
# 如果有 models/ 目录（v5/triad/ensemble）
COPY services/api/models /app/models
# 公共模块（你之前的 db 等）
COPY common /app/common

# 环境
ENV PORT=8080
EXPOSE 8080

# 启动 API；调度器会在 app.py 中根据 START_SCHEDULER=true 自动启动
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
