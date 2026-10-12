FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/ingestion-service/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/ingestion-service/src/ ./src/

# Mount the source files at run time; the database comes from the
# POSTGRES_* environment variables (see .env.example).
ENTRYPOINT ["python", "/app/src/ingest.py"]
