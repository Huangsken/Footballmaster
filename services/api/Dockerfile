# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先装一部分基础依赖（可选，但能更清晰看到失败点）
RUN pip install --no-cache-dir requests==2.31.0 fastapi==0.110.0 uvicorn==0.27.1

# 复制并安装项目依赖（注意：这里的 requirements.txt 在 services/api/ 目录里）
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 复制应用代码（相对路径都是相对 services/api/）
COPY app.py /app/app.py
COPY cron.py /app/cron.py
COPY models /app/models
COPY common /app/common

# 环境
ENV PORT=8080
EXPOSE 8080

# 启动 API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
