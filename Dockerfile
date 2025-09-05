# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先安装三大核心依赖，能更好命中缓存
RUN pip install --no-cache-dir requests==2.31.0 fastapi==0.110.0 uvicorn==0.27.1

# 安装其余依赖
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt && python -m pip show requests

# 复制应用代码
COPY services/api/app.py /app/app.py
COPY services/api/cron.py /app/cron.py
COPY services/api/models /app/models
COPY services/api/common /app/common

# 环境
ENV PORT=8080
EXPOSE 8080

# 启动 API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
