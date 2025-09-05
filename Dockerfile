# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 1) 先复制 requirements.txt 并安装
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 2) 再复制代码
COPY services/api/app.py   /app/app.py
COPY services/api/cron.py  /app/cron.py
COPY services/api/models   /app/models
COPY common                /app/common

# 3) 环境
ENV PORT=8080
EXPOSE 8080

# 4) 启动 API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
