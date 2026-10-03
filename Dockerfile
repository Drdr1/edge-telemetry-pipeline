FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY . .
RUN pip install --no-cache-dir . dagster-webserver
RUN mkdir -p /app/.dagster
