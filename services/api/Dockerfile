# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先安装依赖（requirements.txt 就在 services/api/ 里）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码 —— 注意这里的路径都以 services/api/ 为根
COPY app.py /app/app.py
COPY cron.py /app/cron.py
COPY models /app/models
COPY common /app/common

# 端口
ENV PORT=8080
EXPOSE 8080

# 启动 FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
