"""Tests for tools/sqlite_tool.py and tools/data_tools.csv_to_sqlite."""

from __future__ import annotations

import sqlite3
import pytest

from tools.sqlite_tool import run_query_sqlite, build_sqlite_mcp_server
from tools.data_tools import csv_to_sqlite


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_db(tmp_path):
    """Create a small SQLite database with a 'sales' table."""
    db = tmp_path / "test.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE sales (id INTEGER, product TEXT, amount REAL, region TEXT)"
        )
        conn.executemany(
            "INSERT INTO sales VALUES (?, ?, ?, ?)",
            [
                (1, "Widget", 10.50, "North"),
                (2, "Gadget", 25.00, "South"),
                (3, "Widget", 15.75, "North"),
                (4, "Doohickey", 5.00, "East"),
                (5, "Gadget", 30.00, "North"),
            ],
        )
    return db


@pytest.fixture
def sample_csv(tmp_path):
    """Write a tiny CSV and return its path."""
    p = tmp_path / "data.csv"
    p.write_text("id,name,score\n1,Alice,95\n2,Bob,80\n3,Carol,88\n")
    return p


# ---------------------------------------------------------------------------
# query_sqlite — happy paths
# ---------------------------------------------------------------------------

async def test_select_all(sample_db):
    result = await run_query_sqlite({"db_path": str(sample_db), "sql": "SELECT * FROM sales"})
    text = result["content"][0]["text"]
    assert "Widget" in text
    assert "Gadget" in text
    # Markdown table should have a header separator
    assert "---" in text


async def test_select_aggregation(sample_db):
    result = await run_query_sqlite(
        {
            "db_path": str(sample_db),
            "sql": "SELECT region, COUNT(*) as cnt FROM sales GROUP BY region ORDER BY cnt DESC",
        }
    )
    text = result["content"][0]["text"]
    assert "North" in text
    assert "cnt" in text


async def test_empty_result(sample_db):
    result = await run_query_sqlite(
        {"db_path": str(sample_db), "sql": "SELECT * FROM sales WHERE id = 9999"}
    )
    assert "no rows" in result["content"][0]["text"].lower()


async def test_pipe_escaping(tmp_path):
    """Cell values containing | should be escaped so the table is valid Markdown."""
    db = tmp_path / "pipes.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (val TEXT)")
        conn.execute("INSERT INTO t VALUES ('a|b')")
    result = await run_query_sqlite({"db_path": str(db), "sql": "SELECT * FROM t"})
    text = result["content"][0]["text"]
    assert "a\\|b" in text


# ---------------------------------------------------------------------------
# query_sqlite — error / rejection paths
# ---------------------------------------------------------------------------

async def test_rejects_non_select(sample_db):
    result = await run_query_sqlite(
        {"db_path": str(sample_db), "sql": "DROP TABLE sales"}
    )
    assert "Error" in result["content"][0]["text"]
    assert "SELECT" in result["content"][0]["text"]


async def test_rejects_missing_db():
    result = await run_query_sqlite(
        {"db_path": "/nonexistent/path.db", "sql": "SELECT 1"}
    )
    assert "Error" in result["content"][0]["text"]
    assert "not found" in result["content"][0]["text"]


async def test_rejects_empty_db_path():
    result = await run_query_sqlite({"db_path": "", "sql": "SELECT 1"})
    assert "Error" in result["content"][0]["text"]


async def test_rejects_empty_sql(sample_db):
    result = await run_query_sqlite({"db_path": str(sample_db), "sql": ""})
    assert "Error" in result["content"][0]["text"]


async def test_invalid_sql_returns_error(sample_db):
    result = await run_query_sqlite(
        {"db_path": str(sample_db), "sql": "SELECT * FROM nonexistent_table"}
    )
    assert "Error" in result["content"][0]["text"]


# ---------------------------------------------------------------------------
# csv_to_sqlite
# ---------------------------------------------------------------------------

def test_csv_to_sqlite_creates_table(sample_csv, tmp_path):
    db = tmp_path / "out.db"
    msg = csv_to_sqlite(sample_csv, db, table_name="scores")
    assert "3 rows" in msg
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
    assert rows == 3


def test_csv_to_sqlite_default_table_name(sample_csv, tmp_path):
    db = tmp_path / "out.db"
    csv_to_sqlite(sample_csv, db)
    # Table name defaults to CSV stem ("data")
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    assert rows == 3


def test_csv_to_sqlite_replace(sample_csv, tmp_path):
    db = tmp_path / "out.db"
    csv_to_sqlite(sample_csv, db, table_name="t")
    csv_to_sqlite(sample_csv, db, table_name="t", if_exists="replace")
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert rows == 3  # replaced, not doubled


def test_csv_to_sqlite_append(sample_csv, tmp_path):
    db = tmp_path / "out.db"
    csv_to_sqlite(sample_csv, db, table_name="t")
    csv_to_sqlite(sample_csv, db, table_name="t", if_exists="append")
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert rows == 6  # appended


def test_csv_to_sqlite_missing_csv(tmp_path):
    with pytest.raises(FileNotFoundError):
        csv_to_sqlite(tmp_path / "missing.csv", tmp_path / "out.db")


def test_csv_to_sqlite_bad_if_exists(sample_csv, tmp_path):
    with pytest.raises(ValueError, match="if_exists"):
        csv_to_sqlite(sample_csv, tmp_path / "out.db", if_exists="nope")


# ---------------------------------------------------------------------------
# MCP server construction
# ---------------------------------------------------------------------------

def test_build_sqlite_mcp_server_returns_object():
    server = build_sqlite_mcp_server()
    assert server is not None


async def test_full_csv_query_roundtrip(sample_csv, tmp_path):
    """Import a CSV then query it end-to-end via query_sqlite."""
    db = tmp_path / "rt.db"
    csv_to_sqlite(sample_csv, db, table_name="scores")

    result = await run_query_sqlite(
        {
            "db_path": str(db),
            "sql": "SELECT name, score FROM scores WHERE score > 85 ORDER BY score DESC",
        }
    )
    text = result["content"][0]["text"]
    assert "Alice" in text
    assert "Carol" in text
    assert "Bob" not in text  # score 80, filtered out
