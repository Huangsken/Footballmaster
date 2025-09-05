# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 1) 复制根目录 requirements.txt
COPY requirements.txt /app/requirements.txt

# 2) 安装依赖
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install requests==2.31.0

# 3) 复制代码
COPY services/api/app.py   /app/app.py
COPY services/api/cron.py  /app/cron.py
COPY services/api/models   /app/models
COPY common                /app/common

# 4) 环境变量
ENV PORT=8080
EXPOSE 8080

# 5) 启动 API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
