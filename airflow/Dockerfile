# Airflow with the data-quality pipeline's Python and Java next to it.
#
# Airflow keeps its own libraries; the pipeline (dq.py and the services)
# runs in a separate virtual environment with PySpark, so its pinned
# pandas/numpy/pyarrow never clash with Airflow's. The DAGs call it
# through METAFLOW_PYTHON; Spark jobs go to the spark-master service
# (SPARK_MASTER, see docker-compose.yml).
#
# The packages come from the pipeline image (docker-compose.yml passes it
# in as the "pipeline" build context): both use CPython 3.12 on Debian, and
# PySpark (~450 MB) is not downloaded a second time.

FROM eclipse-temurin:17-jre-jammy AS java

FROM apache/airflow:3.0.6-python3.12

ENV JAVA_HOME=/opt/java/openjdk \
    METAFLOW_JDBC_JAR=/opt/jars/postgresql.jar
ENV PATH=/opt/java/openjdk/bin:$PATH

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /opt/jars/postgresql.jar
USER root
RUN chmod 755 /opt/jars
USER airflow

RUN python -m venv /home/airflow/metaflow-venv
COPY --from=pipeline --chown=airflow:root /usr/local/lib/python3.12/site-packages \
    /home/airflow/metaflow-venv/lib/python3.12/site-packages
