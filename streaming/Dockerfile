# Speed layer: PySpark (local mode) + Java 17
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download the Kafka connector JARs at build time so start-up needs no internet access
RUN python -c "from pyspark.sql import SparkSession; \
s = SparkSession.builder.master('local[1]').config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1').getOrCreate(); s.stop()"
COPY common ./common
COPY streaming ./streaming
CMD ["python", "-m", "streaming.spark_job"]
