# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先安装基本依赖
RUN pip install --no-cache-dir requests==2.31.0 fastapi==0.110.0 uvicorn==0.27.1

# 复制并安装所有依赖
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 复制应用代码
COPY services/api/app.py /app/app.py
COPY services/api/cron.py /app/cron.py
COPY services/api/models /app/models
COPY common /app/common

# 环境变量
ENV PORT=8080
EXPOSE 8080

# 启动API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
