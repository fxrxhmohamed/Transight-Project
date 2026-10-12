FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/dq-service/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/dq-service/src/ ./src/
# Bronze layer client (PostgreSQL) and the dq tables, created on first use
# when the database does not have them yet.
COPY services/ingestion-service/src/bronze.py ./src/
COPY sql/02_dq.sql ./src/

# Every output goes to PostgreSQL (POSTGRES_* variables); mount config/ at
# /app/config at run time.
ENTRYPOINT ["python", "/app/src/dq_engine.py"]
