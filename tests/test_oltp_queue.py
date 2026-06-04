"""
Unit tests for the OLTP job queue using SELECT ... FOR UPDATE SKIP LOCKED.
Synchronous version using psycopg2 (no asyncio required).
Requires a running PostgreSQL instance with a 'transaction_queue' table.
"""

import pytest
import psycopg2
from psycopg2 import sql, extensions

pytestmark = pytest.mark.skip(reason="PostgreSQL not available in CI/local environment")

# ----------------------------------------------------------------------
# Fixture to create a temporary table and clean up after the test
# ----------------------------------------------------------------------
@pytest.fixture(scope="function")
def queue_table():
    """Creates a temporary table and returns a connection and table name."""
    conn = psycopg2.connect(
        user="stripe",
        password="your_secure_password",
        database="stripe_oltp",
        host="localhost"
    )
    conn.autocommit = False
    table_name = "test_transaction_queue"

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TEMPORARY TABLE {table_name} (
                transaction_id SERIAL PRIMARY KEY,
                merchant_id TEXT NOT NULL,
                payload JSONB,
                status TEXT NOT NULL DEFAULT 'pending',
                locked_by TEXT,
                locked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    yield conn, table_name
    conn.rollback()  # discard any changes
    conn.close()


def test_skip_locked_queue(queue_table):
    """
    Simulate two concurrent workers each claiming 3 pending rows.
    We simulate concurrency by using two separate database connections
    and explicit transactions.
    """
    conn, table = queue_table

    # Insert 10 pending rows
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {table} (merchant_id, payload, status)
            SELECT 'test_skip', '{{}}', 'pending'
            FROM generate_series(1, 10)
        """)
    conn.commit()

    def worker(worker_id, limit=3):
        # Each worker uses its own connection to simulate concurrency
        worker_conn = psycopg2.connect(
            user="stripe",
            password="your_secure_password",
            database="stripe_oltp",
            host="localhost"
        )
        worker_conn.autocommit = False
        try:
            with worker_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT transaction_id
                    FROM {table}
                    WHERE status = 'pending' AND merchant_id = 'test_skip'
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (limit,))
                rows = cur.fetchall()
                ids = [r[0] for r in rows]
                if ids:
                    cur.execute(f"""
                        UPDATE {table}
                        SET status = 'processing', locked_by = %s
                        WHERE transaction_id = ANY(%s)
                    """, (worker_id, ids))
                worker_conn.commit()
                return ids
        finally:
            worker_conn.close()

    # Run two workers sequentially but each in its own transaction (simulate concurrency)
    # For a real concurrent test, you would use threads; here we just verify that
    # the SKIP LOCKED logic works when called in order (they won't interfere).
    # The main point is to demonstrate the SQL pattern.
    res1 = worker("w1", 3)
    res2 = worker("w2", 3)

    total_locked = len(res1) + len(res2)
    assert total_locked == 6

    # No duplicate IDs
    all_ids = res1 + res2
    assert len(set(all_ids)) == len(all_ids)

    # Remaining rows should still be pending
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE status = 'pending' AND merchant_id = 'test_skip'
        """)
        remaining = cur.fetchone()[0]
    assert remaining == 4

    # Check processing rows
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE status = 'processing' AND merchant_id = 'test_skip'
        """)
        processing = cur.fetchone()[0]
    assert processing == 6


def test_skip_locked_no_rows_left(queue_table):
    """
    When only 2 rows exist, the first worker should get them, the second gets none.
    """
    conn, table = queue_table

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {table} (merchant_id, payload, status)
            SELECT 'test_empty', '{{}}', 'pending'
            FROM generate_series(1, 2)
        """)
    conn.commit()

    def worker(worker_id, limit=3):
        worker_conn = psycopg2.connect(
            user="stripe",
            password="your_secure_password",
            database="stripe_oltp",
            host="localhost"
        )
        worker_conn.autocommit = False
        try:
            with worker_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT transaction_id
                    FROM {table}
                    WHERE status = 'pending' AND merchant_id = 'test_empty'
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (limit,))
                rows = cur.fetchall()
                ids = [r[0] for r in rows]
                if ids:
                    cur.execute(f"""
                        UPDATE {table}
                        SET status = 'processing', locked_by = %s
                        WHERE transaction_id = ANY(%s)
                    """, (worker_id, ids))
                worker_conn.commit()
                return ids
        finally:
            worker_conn.close()

    res1 = worker("w1")
    res2 = worker("w2")

    assert len(res1) + len(res2) == 2

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE status = 'pending' AND merchant_id = 'test_empty'
        """)
        remaining = cur.fetchone()[0]
    assert remaining == 0