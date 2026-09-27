"""Small PostgreSQL helper with connection retry (containers start in random order)."""
import time

import psycopg2

from common import config


def connect(retries: int = 30, delay: float = 2.0):
    """Return a new psycopg2 connection, retrying while Postgres is still starting."""
    last_err = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                connect_timeout=5,
            )
        except psycopg2.OperationalError as exc:  # not ready yet
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"PostgreSQL not reachable after {retries} attempts: {last_err}")
