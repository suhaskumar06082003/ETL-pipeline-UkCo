# FastAPI ingestion API image.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# api/engine/config/core code + .env are mounted at runtime (see docker-compose.yml)
EXPOSE 8000
CMD ["uvicorn", "api.views:app", "--host", "0.0.0.0", "--port", "8000"]
