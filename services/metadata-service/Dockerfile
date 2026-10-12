FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/metadata-service/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/metadata-service/src/ ./src/
# Bronze layer client (PostgreSQL).
COPY services/ingestion-service/src/bronze.py ./src/

ENTRYPOINT ["python", "/app/src/profiler.py"]
