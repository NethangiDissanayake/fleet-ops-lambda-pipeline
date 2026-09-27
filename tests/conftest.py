"""Shared fixtures.

* Unit tests need nothing but Python.
* Database tests need a PostgreSQL server. Point TEST_PG_ADMIN_DSN at any server where you are a
  superuser, e.g. the one started by docker compose:
      set TEST_PG_ADMIN_DSN=host=localhost port=5433 user=fleet password=fleet dbname=fleet
  Each test module gets its OWN throw-away database (created from db/init/02_schema.sql, dropped afterwards),
  so your real data is never touched. Without the variable those tests are skipped.
* Spark tests are skipped automatically when pyspark or Java is missing.
"""
import os
import pathlib
import sys
import uuid

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SCHEMA = ROOT / "db" / "init" / "02_schema.sql"


@pytest.fixture(scope="module")
def pg_params():
    """Connection parameters of a fresh throw-away database that already contains the schema."""
    dsn = os.environ.get("TEST_PG_ADMIN_DSN")
    if not dsn:
        pytest.skip("TEST_PG_ADMIN_DSN not set - skipping database tests")
    import psycopg2

    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = f"fleet_test_{uuid.uuid4().hex[:8]}"
    with admin.cursor() as cur:
        cur.execute(f"CREATE DATABASE {name}")
    raw = dict(p.split("=", 1) for p in dsn.split())
    params = {"host": raw.get("host"), "port": raw.get("port"), "user": raw.get("user"),
              "password": raw.get("password"), "dbname": name}
    conn = psycopg2.connect(**params)
    with conn.cursor() as cur:
        cur.execute(SCHEMA.read_text())
    conn.commit()
    conn.close()
    yield params
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE {name} WITH (FORCE)")
    admin.close()


@pytest.fixture(scope="module")
def pg_conn(pg_params):
    import psycopg2

    conn = psycopg2.connect(**pg_params)
    yield conn
    conn.close()
