"""FastAPI web UI for the CSV Data Analyst Agent."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from agent import PROJECT_ROOT, run_agent

load_dotenv(".env.local")
load_dotenv()

REPORTS_DIR = PROJECT_ROOT / "reports"
UPLOADS_DIR = PROJECT_ROOT / "data" / "uploads"
STATIC_DIR  = PROJECT_ROOT / "static"
LOGS_DIR    = PROJECT_ROOT / "logs"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for d in [REPORTS_DIR, UPLOADS_DIR, STATIC_DIR, LOGS_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO)
    yield


# Directories must exist before StaticFiles is instantiated below
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="CSV Analyst Agent", lifespan=lifespan)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("unhandled_error path=%s", request.url.path)
    return JSONResponse(status_code=500, content={"detail": str(exc) or "Internal server error"})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/data-files")
async def data_files():
    """List CSV/Excel files in the data/ directory (excluding the uploads subfolder)."""
    data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        return {"files": []}
    files = [
        {"name": f.name, "path": str(f.relative_to(PROJECT_ROOT))}
        for f in sorted(data_dir.iterdir())
        if f.is_file() and f.suffix.lower() in {".csv", ".xlsx", ".xls"}
        and f.parent.name != "uploads"
    ]
    return {"files": files}


@app.post("/analyse")
async def analyse(
    prompt: str = Form(...),
    file: UploadFile = File(default=None),
    data_path: str = Form(default=None),   # path to an existing file in data/
):
    csv_path: Path | None = None

    if file and file.filename:
        # Uploaded file takes priority over dropdown selection
        if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported.")
        dest = UPLOADS_DIR / file.filename
        content = await file.read()
        await asyncio.to_thread(dest.write_bytes, content)
        csv_path = dest
    elif data_path:
        # Existing file selected from the dropdown
        candidate = (PROJECT_ROOT / data_path).resolve()
        try:
            candidate.relative_to(PROJECT_ROOT.resolve())
        except ValueError:
            raise HTTPException(status_code=403, detail="Invalid file path.")
        if not candidate.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {data_path}")
        csv_path = candidate

    # Use a per-request temp audit log so we capture only this run's steps
    LOGS_DIR.mkdir(exist_ok=True)
    audit_path = Path(tempfile.mktemp(suffix=".jsonl", dir=str(LOGS_DIR)))

    try:
        result = await run_agent(
            prompt,
            str(csv_path) if csv_path else None,
            audit_log=str(audit_path),
        )
    except EnvironmentError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("analyse.unexpected_error")
        raise HTTPException(status_code=500, detail=str(exc))

    # Save Markdown report (our own copy of the agent's result text)
    stem = csv_path.stem if csv_path else "report"
    md_path = REPORTS_DIR / f"{stem}.md"
    await asyncio.to_thread(md_path.write_text, result, "utf-8")

    # Parse observability steps from the per-run audit log
    steps = await asyncio.to_thread(parse_audit_steps, audit_path)

    # Discover files the agent actually wrote to reports/ (from audit Write entries)
    agent_files = _files_written_by_agent(audit_path)

    # Build the generated-files list for the UI.
    # Start with the markdown report we saved above, then add everything the agent wrote.
    seen: set[str] = {md_path.name}
    generated_files: list[dict] = [
        {"name": md_path.name, "url": f"/reports/{md_path.name}", "type": "markdown"},
    ]
    for path_str in agent_files:
        p = Path(path_str)
        if not p.exists() or p.name in seen:
            continue
        seen.add(p.name)
        ext = p.suffix.lower()
        if ext in {".xlsx", ".xls"}:
            ftype = "excel"
        elif ext == ".md":
            ftype = "markdown"
        elif ext == ".png":
            ftype = "image"
        else:
            ftype = "other"
        # Only serve files inside REPORTS_DIR
        try:
            p.relative_to(REPORTS_DIR)
            generated_files.append({"name": p.name, "url": f"/reports/{p.name}", "type": ftype})
        except ValueError:
            pass

    # Pick the first Excel the agent (or we) produced for the legacy excel_url field
    excel_url = next(
        (f["url"] for f in generated_files if f["type"] == "excel"), None
    )

    return {
        "result": result,
        "report_url": f"/reports/{md_path.name}",
        "excel_url": excel_url,
        "steps": steps,
        "generated_files": generated_files,
    }


class OpenFileRequest(BaseModel):
    path: str
    action: str = "open"   # "open" | "reveal"


@app.post("/open-file")
async def open_file(req: OpenFileRequest):
    """Open a report file with the default desktop app, or reveal it in Finder."""
    try:
        abs_path = (PROJECT_ROOT / req.path.lstrip("/")).resolve()
        abs_path.relative_to(REPORTS_DIR.resolve())   # raises ValueError if outside
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied: path is outside reports directory")

    if not abs_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    cmd = ["open", "-R", str(abs_path)] if req.action == "reveal" else ["open", str(abs_path)]
    subprocess.Popen(cmd)
    return {"ok": True}


# Mount after all routes so routes take precedence
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")


# ---------------------------------------------------------------------------
# Observability: translate raw audit log → executive-friendly steps
# ---------------------------------------------------------------------------

_STEP_COLORS = {
    "data_access": "blue",
    "analysis":    "violet",
    "aggregation": "emerald",
    "database":    "amber",
    "output":      "teal",
}

_STEP_ICONS = {
    "data_access": "📂",
    "analysis":    "📊",
    "aggregation": "🔢",
    "database":    "🗃️",
    "output":      "📝",
}


def _files_written_by_agent(audit_path: Path) -> list[str]:
    """Return file paths from Write tool entries in the audit log."""
    if not audit_path.exists():
        return []
    paths = []
    for line in audit_path.read_text("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("tool_name") == "Write":
            fp = entry.get("tool_input", {}).get("file_path", "")
            if fp:
                paths.append(fp)
    return paths


def parse_audit_steps(audit_path: Path) -> list[dict]:
    """Read a JSONL audit file and return human-readable step dicts."""
    if not audit_path.exists():
        return []

    steps, n = [], 0
    for line in audit_path.read_text("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        step = _translate_entry(entry)
        if step is None:
            continue

        n += 1
        cat = step.get("category", "analysis")
        step.update({
            "step":        n,
            "color":       _STEP_COLORS.get(cat, "gray"),
            "icon":        _STEP_ICONS.get(cat, "⚙️"),
            "ts":          entry.get("ts", ""),
            "duration_ms": entry.get("duration_ms", 0),
        })
        steps.append(step)

    return steps


def _translate_entry(entry: dict) -> dict | None:
    tool    = entry.get("tool_name", "")
    inp     = entry.get("tool_input", {})
    preview = entry.get("result_preview", "")

    # ---- Read ---------------------------------------------------------------
    if tool == "Read":
        path = inp.get("file_path", "")
        name = Path(path).name if path else ""
        if path.lower().endswith((".csv", ".xlsx", ".xls")):
            return {
                "title":    f"Accessed data file: {name}",
                "detail":   f"Opened {name} from {Path(path).parent}",
                "category": "data_access",
            }
        return None

    # ---- Write --------------------------------------------------------------
    if tool == "Write":
        path = inp.get("file_path", "")
        name = Path(path).name if path else ""
        if name.endswith(".md"):
            return {
                "title":    "Saved analysis report",
                "detail":   f"Wrote the final Markdown report to {name}",
                "category": "output",
            }
        if name.endswith((".xlsx", ".xls")):
            return {
                "title":    "Saved Excel workbook",
                "detail":   f"Generated multi-sheet Excel file: {name}",
                "category": "output",
            }
        return None

    # ---- Bash ---------------------------------------------------------------
    if tool == "Bash":
        return _translate_bash(inp.get("command", ""), entry.get("duration_ms", 0), preview)

    # ---- SQLite MCP tool ----------------------------------------------------
    if "sqlite" in tool.lower() and "query" in tool.lower():
        sql = inp.get("sql", inp.get("query", ""))
        if sql:
            return _translate_sql(sql)

    return None


# Commands that are internal housekeeping — skip them
_SKIP_BASH = [
    "uv add", "importlib", "ls -la", "ls -lh", "echo ", "kill ",
    "lsof", "pip install", "which ", "cat ", "head ", "tail ",
]


def _translate_bash(cmd: str, duration_ms: int, preview: str) -> dict | None:
    if any(s in cmd for s in _SKIP_BASH):
        return None

    lo = cmd.lower()

    # CSV → SQLite import
    if "csv_to_sqlite" in cmd or ("to_sql(" in cmd and "sqlite" in lo):
        return {
            "title":    "Imported data into analysis database",
            "detail":   "Loaded the data file into a structured SQLite database, "
                        "enabling fast SQL queries and multi-dimensional analysis",
            "category": "database",
        }

    # Chart / visualisation generation
    if any(x in lo for x in ["savefig", "matplotlib", "seaborn"]):
        n_charts = len(re.findall(r"Chart \d+ saved", preview))
        count_str = f"{n_charts} chart{'s' if n_charts != 1 else ''}" if n_charts else "charts"
        return {
            "title":    f"Generated {count_str} — bar, trend, and breakdown visualisations",
            "detail":   "Produced visual summaries of key metrics, "
                        "embedded into the Excel workbook for at-a-glance insight",
            "category": "output",
        }

    # Dataset profiling
    if "describe(" in cmd and "isnull" in cmd:
        shape = re.search(r"Rows:\s*([\d,]+),\s*Columns:\s*(\d+)", preview)
        if shape:
            detail = (f"Dataset contains {int(shape.group(1).replace(',', '')):,} records "
                      f"across {shape.group(2)} fields — examined data types, value ranges, "
                      f"and completeness")
        else:
            detail = ("Examined column data types, value ranges, missing values, "
                      "and statistical distributions across all fields")
        return {
            "title":    "Profiled dataset structure and data quality",
            "detail":   detail,
            "category": "analysis",
        }

    # Aggregation / groupby
    if "groupby(" in cmd:
        cols = _extract_groupby_cols(cmd)
        fns  = _extract_agg_fns(cmd)
        col_str = " and ".join(cols[:3]) if cols else "key dimensions"
        fn_str  = ", ".join(fns)         if fns  else "key metrics"
        return {
            "title":    f"Computed {fn_str} broken down by {col_str}",
            "detail":   f"Aggregated the dataset by {col_str} to identify "
                        f"top performers, patterns, and trends",
            "category": "aggregation",
        }

    # SQL queries embedded inside Python (sqlite3 / read_sql)
    if "sqlite3" in cmd or "read_sql(" in cmd:
        sql_m = re.search(r'"""\s*(SELECT.*?)"""', cmd, re.DOTALL | re.IGNORECASE)
        if not sql_m:
            sql_m = re.search(r"'''\s*(SELECT.*?)'''", cmd, re.DOTALL | re.IGNORECASE)
        if sql_m:
            return _translate_sql(sql_m.group(1))
        return {
            "title":    "Queried database for cross-dimensional insights",
            "detail":   "Ran structured SQL queries to extract ranked results, "
                        "breakdowns, and trend data",
            "category": "database",
        }

    # General pandas work
    if "pandas" in lo or "pd." in cmd or "read_csv" in lo:
        return {
            "title":    "Processed and transformed data",
            "detail":   "Applied data cleaning, type conversions, and calculations "
                        "using the pandas analytics library",
            "category": "analysis",
        }

    return None


def _translate_sql(sql: str) -> dict:
    up = sql.upper().replace("\n", " ")

    gm = re.search(r"GROUP\s+BY\s+([\w\s,]+?)(?:\s+ORDER|\s+HAVING|\s+LIMIT|$)", up)
    groups = [c.strip().lower() for c in gm.group(1).split(",") if c.strip()] if gm else []

    agg_labels = [
        label for fn, label in [
            ("SUM(",   "totals"),
            ("AVG(",   "averages"),
            ("COUNT(", "counts"),
            ("MAX(",   "maximums"),
            ("MIN(",   "minimums"),
        ] if fn in up
    ]

    tm = re.search(r"FROM\s+(\w+)", up)
    table = tm.group(1).lower() if tm else "the dataset"

    if groups and agg_labels:
        col_str = " and ".join(groups[:3])
        agg_str = ", ".join(agg_labels[:3])
        return {
            "title":    f"Queried {table}: computed {agg_str} by {col_str}",
            "detail":   f"SQL aggregation — grouped {table} by {col_str} to calculate {agg_str}",
            "category": "database",
        }
    if groups:
        return {
            "title":    f"Queried {table}: grouped by {', '.join(groups[:3])}",
            "detail":   f"Retrieved a structured breakdown from the {table} table",
            "category": "database",
        }
    return {
        "title":    f"Queried {table} for detailed records",
        "detail":   "Retrieved specific records and computed metrics from the analysis database",
        "category": "database",
    }


def _extract_groupby_cols(cmd: str) -> list[str]:
    cols: list[str] = []
    for m in re.findall(r'groupby\(\s*[\[\"\']([^\'\"\]]+)[\"\'\]]\s*\)', cmd):
        cols.extend(re.findall(r'\w+', m))
    return list(dict.fromkeys(cols))[:4]


def _extract_agg_fns(cmd: str) -> list[str]:
    patterns = [
        (r'"sum"|\'sum\'|\bsum\b', "totals"),
        (r'"mean"|\'mean\'|\bmean\b|\bavg\b', "averages"),
        (r'"count"|\'count\'|\bcount\b', "counts"),
        (r'"max"|\'max\'|\bmax\b', "maximums"),
        (r'"min"|\'min\'|\bmin\b', "minimums"),
        (r'\bstd\b|\bstddev\b', "standard deviations"),
        (r'\bmedian\b', "medians"),
    ]
    seen: set[str] = set()
    fns: list[str] = []
    for pat, label in patterns:
        if re.search(pat, cmd) and label not in seen:
            fns.append(label)
            seen.add(label)
    return fns[:4]
