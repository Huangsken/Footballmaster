# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 先把 requirements.txt 拷贝进去再安装
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 再拷贝代码
COPY services/api/app.py /app/app.py
COPY services/api/cron.py /app/cron.py
COPY services/api/models /app/models
COPY common /app/common

# 环境
ENV PORT=8080
EXPOSE 8080

# 启动 API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
