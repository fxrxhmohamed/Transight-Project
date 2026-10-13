# Runtime of every pipeline step (dq.py), the tests and the correction UI:
# Python, Java for Spark, PySpark and the PostgreSQL JDBC driver.
#
#   docker compose build pipeline
#   docker compose run --rm pipeline python dq.py --help
#
# The project folder is mounted at /opt/metaflow at run time (code, config,
# source files); every output goes to PostgreSQL. The Airflow image copies
# the Python packages from this image (PySpark is ~450 MB: downloaded once).

FROM eclipse-temurin:17-jre-jammy AS java

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    METAFLOW_JDBC_JAR=/opt/jars/postgresql.jar

# Java 17 for Spark (copied from the Temurin image: no package manager
# needed) and the PostgreSQL JDBC driver.
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /opt/jars/postgresql.jar

COPY services/ingestion-service/requirements.txt /tmp/requirements-ingestion.txt
COPY services/metadata-service/requirements.txt /tmp/requirements-metadata.txt
COPY services/dq-service/requirements.txt /tmp/requirements-dq.txt
RUN pip install --no-cache-dir \
        -r /tmp/requirements-ingestion.txt \
        -r /tmp/requirements-metadata.txt \
        -r /tmp/requirements-dq.txt \
        pytest==9.1.1

WORKDIR /opt/metaflow
CMD ["python", "dq.py", "--help"]
