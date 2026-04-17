#!/usr/bin/env python3
"""
New York Amsterdam News Archive — web interface.
Supports SQLite (local) and PostgreSQL/Supabase (production).
Run locally:  python3 app.py
"""

import os
from pathlib import Path
from flask import Flask, render_template, request, jsonify, g

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip().strip('"').strip("'") or None
SQLITE_PATH  = Path(os.environ.get("DB_PATH", Path(__file__).parent / "amsterdam_news.db"))

USE_POSTGRES = bool(DATABASE_URL)

app = Flask(__name__)


# ── Database connection ────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        if USE_POSTGRES:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        else:
            import sqlite3
            g.db = sqlite3.connect(SQLITE_PATH)
            g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db:
        db.close()


def query(sql, params=()):
    if USE_POSTGRES:
        sql = sql.replace("?", "%s")
    db = get_db()
    cur = db.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def queryrow(sql, params=()):
    rows = query(sql, params)
    return rows[0] if rows else None


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    stats = queryrow("""
        SELECT
            (SELECT COUNT(*) FROM issues)      AS total_issues,
            (SELECT COUNT(*) FROM issue_text)  AS indexed,
            (SELECT MIN(pub_date) FROM issues WHERE pub_date NOT LIKE '%-00-%') AS earliest,
            (SELECT MAX(pub_date) FROM issues WHERE pub_date NOT LIKE '%-00-%') AS latest
    """)
    years = query("""
        SELECT year, COUNT(*) as issue_count
        FROM issues
        WHERE year > 0
        GROUP BY year
        ORDER BY year
    """)
    return render_template("index.html", stats=stats, years=years)


@app.route("/search")
def search():
    q        = request.args.get("q", "").strip()
    year     = request.args.get("year", "")
    page     = int(request.args.get("page", 1))
    per_page = 10
    offset   = (page - 1) * per_page

    all_years = query("""
        SELECT DISTINCT year FROM issues WHERE year > 0 ORDER BY year
    """)

    if not q:
        return render_template("search.html", query="", results=[], total=0,
                               page=1, pages=1, indexed=0, years=all_years)

    indexed = (queryrow("SELECT COUNT(*) AS n FROM issue_text") or {}).get("n", 0)

    if USE_POSTGRES:
        results, total = _search_postgres(q, year, per_page, offset)
    else:
        results, total = _search_sqlite(q, year, per_page, offset)

    pages = max(1, (total + per_page - 1) // per_page)
    return render_template("search.html",
        query=q, results=results, total=total,
        page=page, pages=pages, per_page=per_page,
        year=year, indexed=indexed, years=all_years,
    )


def _search_postgres(q, year, per_page, offset):
    where  = ["it.search_vec @@ plainto_tsquery('english', %s)"]
    params = [q]
    if year:
        where.append("i.year = %s"); params.append(int(year))

    where_str = " AND ".join(where)
    total = (queryrow(f"""
        SELECT COUNT(*) AS n FROM issue_text it
        JOIN issues i ON i.id = it.issue_id
        WHERE {where_str}
    """, params) or {}).get("n", 0)

    results = query(f"""
        SELECT i.id, i.pub_date, i.year, i.month, i.day, i.page_count, i.url,
               ts_headline('english', it.full_text,
                   plainto_tsquery('english', %s),
                   'MaxWords=50, MinWords=20, StartSel=<mark>, StopSel=</mark>, MaxFragments=1'
               ) AS snippet
        FROM issue_text it
        JOIN issues i ON i.id = it.issue_id
        WHERE {where_str}
        ORDER BY ts_rank(it.search_vec, plainto_tsquery('english', %s)) DESC
        LIMIT %s OFFSET %s
    """, [q] + params + [q, per_page, offset])
    return results, total


def _search_sqlite(q, year, per_page, offset):
    where  = ["issues_fts MATCH ?"]
    params = [q]
    if year:
        where.append("i.year = ?"); params.append(int(year))

    where_str = " AND ".join(where)
    total = (queryrow(f"""
        SELECT COUNT(*) AS n FROM issues_fts fts
        JOIN issues i ON fts.rowid = i.id
        WHERE {where_str}
    """, params) or {}).get("n", 0)

    results = query(f"""
        SELECT i.id, i.pub_date, i.year, i.month, i.day, i.page_count, i.url,
               snippet(issues_fts, 0, '<mark>', '</mark>', '…', 48) AS snippet
        FROM issues_fts fts
        JOIN issues i ON fts.rowid = i.id
        LEFT JOIN issue_text it ON it.issue_id = i.id
        WHERE {where_str}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])
    return results, total


@app.route("/browse")
def browse():
    year   = request.args.get("year", str(
        (queryrow("SELECT MIN(year) as y FROM issues WHERE year > 0") or {}).get("y", 1963)
    ))
    issues = query("""
        SELECT i.id, i.pub_date, i.year, i.month, i.day, i.page_count, i.url,
               CASE WHEN it.issue_id IS NOT NULL THEN 1 ELSE 0 END as indexed
        FROM issues i
        LEFT JOIN issue_text it ON it.issue_id = i.id
        WHERE i.year = ? AND i.pub_date NOT LIKE '%-00-%'
        ORDER BY i.pub_date
    """, (year,))
    all_years = query("SELECT DISTINCT year FROM issues WHERE year > 0 ORDER BY year")
    return render_template("browse.html", issues=issues, all_years=all_years, current_year=int(year))


@app.route("/issue/<int:issue_id>")
def issue_detail(issue_id):
    issue = queryrow("""
        SELECT i.*, it.full_text, it.indexed_at
        FROM issues i
        LEFT JOIN issue_text it ON it.issue_id = i.id
        WHERE i.id = ?
    """, (issue_id,))
    if not issue:
        return "Issue not found", 404

    pages = query("""
        SELECT page_num, url FROM pages WHERE issue_id = ? ORDER BY page_num
    """, (issue_id,))

    return render_template("issue.html", issue=issue, pages=pages)


@app.route("/api/progress")
def api_progress():
    row = queryrow("""
        SELECT (SELECT COUNT(*) FROM issues)     AS total,
               (SELECT COUNT(*) FROM issue_text) AS indexed
    """)
    return jsonify({"total": row["total"], "indexed": row["indexed"]})


if __name__ == "__main__":
    print(f"Mode: {'PostgreSQL (Supabase)' if USE_POSTGRES else 'SQLite (local)'}")
    print("Running at http://localhost:5051")
    app.run(port=5051, debug=False)
