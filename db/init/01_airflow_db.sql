-- Airflow keeps its own metadata (DAG runs, task states) in a separate database.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
