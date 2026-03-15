"""Integration tests — run the agent on data/sample_sales.csv.

These tests require a valid ANTHROPIC_API_KEY in the environment (or .env.local).
They are skipped automatically when the key is absent so CI passes without
credentials.

Run manually with:
    uv run pytest tests/test_integration.py -v
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

import pytest

# Skip the entire module if no API key is configured.
pytestmark = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set — skipping integration tests",
)

DATA_PATH = Path(__file__).parent.parent / "data" / "sample_sales.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_report(tmp_path: Path) -> str:
    """Run the agent and return the result text."""
    # Import here so the module-level skip fires before any SDK imports fail.
    from dotenv import load_dotenv
    load_dotenv(".env.local")
    load_dotenv()

    import anyio
    from agent import run_agent

    result = anyio.run(
        run_agent,
        "Analyse the sales data. Produce a summary report with key statistics.",
        str(DATA_PATH),
        max_turns=20,
        max_budget_usd=0.50,
        audit_log=str(tmp_path / "audit.jsonl"),
    )
    return result


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestAgentReportContent:
    """Verify the agent produces a meaningful Markdown report."""

    @pytest.fixture(scope="class")
    def report(self, tmp_path_factory):
        tmp_path = tmp_path_factory.mktemp("integration")
        return _get_report(tmp_path)

    def test_result_is_string(self, report):
        assert isinstance(report, str)

    def test_result_not_empty(self, report):
        assert len(report.strip()) > 100

    def test_mentions_row_count(self, report):
        """Report should mention 30 rows (the sample data has 30 rows)."""
        # Accept "30" alone or in context like "30 rows" / "30 records"
        assert re.search(r"\b30\b", report), f"'30' not found in report:\n{report[:500]}"

    def test_mentions_numeric_summary(self, report):
        """Report should include numeric statistics."""
        lower = report.lower()
        has_stat = any(kw in lower for kw in ("mean", "average", "total", "sum", "median"))
        assert has_stat, "No numeric statistics found in report"

    def test_mentions_product_or_category(self, report):
        """Report should reference at least one product or category."""
        lower = report.lower()
        products = ("widget", "gadget", "chair", "lamp", "notebook",
                    "electronics", "furniture", "stationery")
        found = any(p in lower for p in products)
        assert found, "No product/category names found in report"

    def test_mentions_region(self, report):
        """Report should reference at least one sales region."""
        lower = report.lower()
        regions = ("north", "south", "east", "west")
        found = any(r in lower for r in regions)
        assert found, "No region names found in report"

    def test_contains_markdown_structure(self, report):
        """Report should use Markdown headings or lists."""
        has_heading = "#" in report
        has_bullet = re.search(r"^[-*]", report, re.MULTILINE) is not None
        assert has_heading or has_bullet, "No Markdown structure (headings/bullets) in report"

    def test_no_error_messages(self, report):
        """Result should not contain raw error/exception messages."""
        lower = report.lower()
        assert "traceback" not in lower
        assert "exception" not in lower


class TestAgentDataIngestion:
    """Verify the agent correctly reads and imports the CSV data."""

    def test_sample_csv_exists(self):
        assert DATA_PATH.exists(), f"Sample data file missing: {DATA_PATH}"

    def test_sample_csv_has_expected_shape(self):
        import pandas as pd
        df = pd.read_csv(DATA_PATH)
        assert df.shape[0] == 30, f"Expected 30 rows, got {df.shape[0]}"
        assert df.shape[1] == 9, f"Expected 9 columns, got {df.shape[1]}"

    def test_sample_csv_columns(self):
        import pandas as pd
        df = pd.read_csv(DATA_PATH)
        expected = {"order_id", "date", "product", "category", "region",
                    "quantity", "unit_price", "total_amount", "customer_id"}
        assert set(df.columns) == expected

    def test_sample_csv_no_nulls(self):
        import pandas as pd
        df = pd.read_csv(DATA_PATH)
        assert df.isnull().sum().sum() == 0, "Unexpected null values in sample data"

    def test_sample_csv_total_revenue(self):
        """Total revenue across all 30 orders."""
        import pandas as pd
        df = pd.read_csv(DATA_PATH)
        total = df["total_amount"].sum()
        # Sum is deterministic; verify it's in expected ballpark
        assert total > 1000, "Total revenue unexpectedly low"
        assert total < 100_000, "Total revenue unexpectedly high"


class TestCsvToSqliteIntegration:
    """Verify the csv_to_sqlite helper works with the real sample data."""

    def test_import_sample_sales(self, tmp_path):
        from tools.data_tools import csv_to_sqlite
        db = tmp_path / "sales.db"
        result = csv_to_sqlite(DATA_PATH, db, table_name="sales")
        assert "30" in result
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        assert count == 30

    def test_query_sqlite_on_sample_data(self, tmp_path):
        """query_sqlite tool returns correct aggregate from sample data."""
        import anyio
        from tools.data_tools import csv_to_sqlite
        from tools.sqlite_tool import run_query_sqlite

        db = tmp_path / "sales.db"
        csv_to_sqlite(DATA_PATH, db, table_name="sales")

        result = anyio.run(
            run_query_sqlite,
            {"db_path": str(db), "sql": "SELECT COUNT(*) AS n FROM sales"},
        )
        text = result["content"][0]["text"]
        assert "30" in text

    def test_query_sqlite_aggregation(self, tmp_path):
        """Verify group-by aggregation works on the sample data."""
        import anyio
        from tools.data_tools import csv_to_sqlite
        from tools.sqlite_tool import run_query_sqlite

        db = tmp_path / "sales.db"
        csv_to_sqlite(DATA_PATH, db, table_name="sales")

        result = anyio.run(
            run_query_sqlite,
            {
                "db_path": str(db),
                "sql": (
                    "SELECT category, COUNT(*) AS orders "
                    "FROM sales GROUP BY category ORDER BY orders DESC"
                ),
            },
        )
        text = result["content"][0]["text"]
        assert "Electronics" in text
        assert "Furniture" in text
