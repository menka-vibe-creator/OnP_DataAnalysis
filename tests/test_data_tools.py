"""Comprehensive tests for tools/data_tools.py."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from tools.data_tools import csv_to_sqlite, load_dataframe, summarise_dataframe


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_csv(tmp_path):
    """Three-row CSV with a missing value."""
    content = "id,value,category\n1,10.5,A\n2,20.0,B\n3,,A\n"
    p = tmp_path / "sample.csv"
    p.write_text(content)
    return p


@pytest.fixture
def sales_csv(tmp_path):
    """Realistic multi-column sales CSV."""
    content = (
        "order_id,date,product,region,quantity,unit_price,total_amount\n"
        "1001,2024-01-05,Widget A,North,3,29.99,89.97\n"
        "1002,2024-01-07,Gadget B,South,1,149.99,149.99\n"
        "1003,2024-01-10,Widget A,East,5,29.99,149.95\n"
        "1004,2024-01-12,Office Chair,West,2,199.00,398.00\n"
        "1005,2024-01-15,Desk Lamp,North,4,39.99,159.96\n"
    )
    p = tmp_path / "sales.csv"
    p.write_text(content)
    return p


@pytest.fixture
def unicode_csv(tmp_path):
    """CSV with Unicode characters."""
    content = "name,city,amount\nÁlvaro,São Paulo,100.0\n田中,東京,200.0\n"
    p = tmp_path / "unicode.csv"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture
def all_numeric_csv(tmp_path):
    """CSV with only numeric columns."""
    content = "x,y,z\n1,2,3\n4,5,6\n7,8,9\n"
    p = tmp_path / "numeric.csv"
    p.write_text(content)
    return p


@pytest.fixture
def all_nan_col_csv(tmp_path):
    """CSV where one column is entirely NaN."""
    content = "a,b,c\n1,,3\n4,,6\n7,,9\n"
    p = tmp_path / "all_nan.csv"
    p.write_text(content)
    return p


@pytest.fixture
def empty_csv(tmp_path):
    """CSV with header only (zero rows)."""
    p = tmp_path / "empty.csv"
    p.write_text("col1,col2,col3\n")
    return p


@pytest.fixture
def single_row_csv(tmp_path):
    """CSV with exactly one data row."""
    p = tmp_path / "one_row.csv"
    p.write_text("a,b\n42,hello\n")
    return p


# ---------------------------------------------------------------------------
# load_dataframe — CSV
# ---------------------------------------------------------------------------

class TestLoadDataframeCsv:

    def test_returns_dataframe(self, simple_csv):
        df = load_dataframe(simple_csv)
        assert isinstance(df, pd.DataFrame)

    def test_correct_shape(self, simple_csv):
        df = load_dataframe(simple_csv)
        assert df.shape == (3, 3)

    def test_column_names(self, simple_csv):
        df = load_dataframe(simple_csv)
        assert list(df.columns) == ["id", "value", "category"]

    def test_missing_values_preserved(self, simple_csv):
        df = load_dataframe(simple_csv)
        assert df["value"].isna().sum() == 1

    def test_accepts_string_path(self, simple_csv):
        df = load_dataframe(str(simple_csv))
        assert df.shape[0] == 3

    def test_accepts_pathlib_path(self, simple_csv):
        df = load_dataframe(Path(simple_csv))
        assert df.shape[0] == 3

    def test_raises_for_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_dataframe(tmp_path / "nonexistent.csv")

    def test_sales_csv_shape(self, sales_csv):
        df = load_dataframe(sales_csv)
        assert df.shape == (5, 7)

    def test_unicode_csv(self, unicode_csv):
        df = load_dataframe(unicode_csv)
        assert df.shape == (2, 3)
        assert "Álvaro" in df["name"].values

    def test_empty_csv_has_columns(self, empty_csv):
        df = load_dataframe(empty_csv)
        assert df.shape == (0, 3)
        assert list(df.columns) == ["col1", "col2", "col3"]


# ---------------------------------------------------------------------------
# load_dataframe — Excel
# ---------------------------------------------------------------------------

class TestLoadDataframeExcel:

    def test_loads_xlsx(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl", reason="openpyxl not installed")
        p = tmp_path / "test.xlsx"
        df_orig = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df_orig.to_excel(p, index=False)
        df = load_dataframe(p)
        assert list(df.columns) == ["a", "b"]
        assert df.shape == (2, 2)

    def test_xlsx_values_preserved(self, tmp_path):
        pytest.importorskip("openpyxl", reason="openpyxl not installed")
        p = tmp_path / "values.xlsx"
        df_orig = pd.DataFrame({"x": [10, 20, 30]})
        df_orig.to_excel(p, index=False)
        df = load_dataframe(p)
        assert list(df["x"]) == [10, 20, 30]


# ---------------------------------------------------------------------------
# summarise_dataframe
# ---------------------------------------------------------------------------

class TestSummariseDataframe:

    def test_returns_string(self, simple_csv):
        df = load_dataframe(simple_csv)
        assert isinstance(summarise_dataframe(df), str)

    def test_contains_shape(self, simple_csv):
        df = load_dataframe(simple_csv)
        s = summarise_dataframe(df)
        assert "3 rows" in s
        assert "3 columns" in s

    def test_contains_column_names(self, simple_csv):
        df = load_dataframe(simple_csv)
        s = summarise_dataframe(df)
        assert "id" in s
        assert "value" in s
        assert "category" in s

    def test_contains_missing_values_section(self, simple_csv):
        df = load_dataframe(simple_csv)
        s = summarise_dataframe(df)
        assert "Missing values" in s

    def test_missing_count_reported(self, simple_csv):
        df = load_dataframe(simple_csv)
        s = summarise_dataframe(df)
        # value column has 1 missing
        assert "1" in s

    def test_contains_dtypes(self, simple_csv):
        df = load_dataframe(simple_csv)
        s = summarise_dataframe(df)
        assert "Data types" in s

    def test_numeric_summary_present(self, all_numeric_csv):
        df = load_dataframe(all_numeric_csv)
        s = summarise_dataframe(df)
        assert "Numeric summary" in s
        assert "mean" in s.lower() or "std" in s.lower()

    def test_all_nan_column(self, all_nan_col_csv):
        df = load_dataframe(all_nan_col_csv)
        s = summarise_dataframe(df)
        # Column b has 3 missing values
        assert "3" in s

    def test_empty_dataframe(self, empty_csv):
        df = load_dataframe(empty_csv)
        s = summarise_dataframe(df)
        assert "0 rows" in s
        assert "3 columns" in s
        assert "no numeric columns" in s or "Numeric summary" in s

    def test_single_row(self, single_row_csv):
        df = load_dataframe(single_row_csv)
        s = summarise_dataframe(df)
        assert "1 rows" in s

    def test_sales_csv_summary(self, sales_csv):
        df = load_dataframe(sales_csv)
        s = summarise_dataframe(df)
        assert "5 rows" in s
        assert "7 columns" in s
        assert "total_amount" in s


# ---------------------------------------------------------------------------
# csv_to_sqlite
# ---------------------------------------------------------------------------

class TestCsvToSqlite:

    def test_creates_database_file(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db)
        assert db.exists()

    def test_returns_summary_string(self, simple_csv, tmp_path):
        result = csv_to_sqlite(simple_csv, tmp_path / "out.db")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_default_table_name_from_stem(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db)
        with sqlite3.connect(db) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
        assert "sample" in tables

    def test_custom_table_name(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db, table_name="my_data")
        with sqlite3.connect(db) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
        assert "my_data" in tables

    def test_correct_row_count(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db)
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
        assert count == 3

    def test_replace_mode_overwrites(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db, if_exists="replace")
        csv_to_sqlite(simple_csv, db, if_exists="replace")
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
        assert count == 3

    def test_append_mode_doubles_rows(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db, if_exists="replace")
        csv_to_sqlite(simple_csv, db, if_exists="append")
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
        assert count == 6

    def test_fail_mode_raises_on_existing_table(self, simple_csv, tmp_path):
        db = tmp_path / "out.db"
        csv_to_sqlite(simple_csv, db, if_exists="replace")
        with pytest.raises(Exception):
            csv_to_sqlite(simple_csv, db, if_exists="fail")

    def test_raises_for_missing_csv(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            csv_to_sqlite(tmp_path / "missing.csv", tmp_path / "out.db")

    def test_raises_for_bad_if_exists(self, simple_csv, tmp_path):
        with pytest.raises(ValueError, match="if_exists"):
            csv_to_sqlite(simple_csv, tmp_path / "out.db", if_exists="overwrite")

    def test_creates_parent_dirs(self, simple_csv, tmp_path):
        db = tmp_path / "subdir" / "nested" / "out.db"
        csv_to_sqlite(simple_csv, db)
        assert db.exists()

    def test_sales_csv_import(self, sales_csv, tmp_path):
        db = tmp_path / "sales.db"
        result = csv_to_sqlite(sales_csv, db)
        assert "5" in result
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        assert count == 5

    def test_columns_match_csv(self, sales_csv, tmp_path):
        db = tmp_path / "sales.db"
        csv_to_sqlite(sales_csv, db)
        with sqlite3.connect(db) as conn:
            cursor = conn.execute("SELECT * FROM sales LIMIT 1")
            cols = [d[0] for d in cursor.description]
        assert cols == ["order_id", "date", "product", "region", "quantity",
                        "unit_price", "total_amount"]

    def test_unicode_content(self, unicode_csv, tmp_path):
        db = tmp_path / "unicode.db"
        csv_to_sqlite(unicode_csv, db, table_name="people")
        with sqlite3.connect(db) as conn:
            rows = conn.execute("SELECT name FROM people").fetchall()
        names = [r[0] for r in rows]
        assert "Álvaro" in names
        assert "田中" in names

    def test_result_mentions_row_count(self, simple_csv, tmp_path):
        result = csv_to_sqlite(simple_csv, tmp_path / "out.db")
        assert "3" in result

    def test_result_mentions_table_name(self, simple_csv, tmp_path):
        result = csv_to_sqlite(simple_csv, tmp_path / "out.db", table_name="my_table")
        assert "my_table" in result
