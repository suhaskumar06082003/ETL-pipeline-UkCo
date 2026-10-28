# ETL Airflow image: Airflow + the etl_project pipeline's runtime deps.
# Deps are installed against Airflow's own constraints so Airflow's pinned
# dependencies (SQLAlchemy, pandas, etc.) are never broken.
FROM apache/airflow:2.10.4-python3.12

# PySpark (naturo_bigqry_fullload_spark) needs a Java runtime + the MySQL JDBC driver
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/mysql-connector-j.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_MYSQL_JAR=/opt/spark-jars/mysql-connector-j.jar
USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt" \
    pandas \
    pyarrow \
    openpyxl \
    chardet \
    azure-storage-blob \
    PyMySQL \
    pydantic-settings \
    deep-translator \
    google-cloud-bigquery \
    pyspark
