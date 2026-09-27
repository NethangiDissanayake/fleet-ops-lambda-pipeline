# Apache Airflow + pyarrow (to read the Parquet master dataset). Installed with Airflow's constraint file
# so dependency versions stay compatible.
FROM apache/airflow:2.9.3-python3.11
RUN pip install --no-cache-dir pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
