from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from app.ch_client import get_client
from app.ingest import fetch_and_insert, fetch_astros

app = FastAPI(title="Astros Ingest Service", version="1.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/raw")
def raw():
    try:
        payload, attempts = fetch_astros("http://api.open-notify.org/astros.json")
        return {"status": "ok", "attempts": attempts, "payload": payload}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/ingest")
def ingest():
    try:
        result = fetch_and_insert()
        return {
            "status": "ok",
            "attempts": result.attempts,
            "inserted_rows": result.inserted_rows,
            "raw_id": result.raw_id,
            "inserted_at": result.inserted_at,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/people")
def people(limit: int = Query(100, ge=1, le=1000)):
    try:
        client = get_client()
        result = client.query(
            "SELECT craft, name, _inserted_at FROM people ORDER BY _inserted_at DESC LIMIT %(limit)s",
            parameters={"limit": limit},
        )
        rows = [
            {"craft": row[0], "name": row[1], "_inserted_at": str(row[2])}
            for row in result.result_rows
        ]
        return {"rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/stats/people_by_craft")
def people_by_craft():
    try:
        client = get_client()
        result = client.query(
            "SELECT craft, people_count, last_seen_at FROM people_by_craft ORDER BY craft"
        )
        rows = [
            {"craft": row[0], "people_count": int(row[1]), "last_seen_at": str(row[2])}
            for row in result.result_rows
        ]
        return {"rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/ui", response_class=HTMLResponse)
def ui():
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Astros Dashboard</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #0f172a;
        --card: #111827;
        --text: #e5e7eb;
        --muted: #9ca3af;
        --accent: #22c55e;
      }
      body {
        margin: 0;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
        background: radial-gradient(1200px 700px at 10% 10%, #1f2937, #0f172a);
        color: var(--text);
      }
      .wrap { max-width: 1000px; margin: 32px auto; padding: 0 16px; }
      h1 { font-size: 28px; margin: 0 0 8px; }
      p { margin: 0 0 16px; color: var(--muted); }
      .grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
      .card { background: var(--card); border: 1px solid #1f2937; border-radius: 12px; padding: 16px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; padding: 8px; border-bottom: 1px solid #1f2937; font-size: 14px; }
      th { color: var(--muted); font-weight: 600; }
      .btn { background: var(--accent); color: #0b1218; border: 0; padding: 8px 12px; border-radius: 8px; font-weight: 600; cursor: pointer; }
      .status { margin-left: 8px; color: var(--muted); font-size: 12px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>Astros Dashboard</h1>
      <p>Live data from ClickHouse via FastAPI.</p>

      <div class="grid">
        <div class="card">
          <h3>People By Craft</h3>
          <table id="craft-table">
            <thead>
              <tr><th>Craft</th><th>People</th><th>Last Seen</th></tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>

        <div class="card">
          <h3>Latest People</h3>
          <table id="people-table">
            <thead>
              <tr><th>Name</th><th>Craft</th><th>Inserted</th></tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>

      <div class="card" style="margin-top:16px;">
        <button class="btn" id="refresh">Refresh</button>
        <span class="status" id="status">idle</span>
      </div>
    </div>

    <script>
      async function loadData() {
        const status = document.getElementById('status');
        status.textContent = 'loading...';
        try {
          const [craftRes, peopleRes] = await Promise.all([
            fetch('/stats/people_by_craft'),
            fetch('/people?limit=12')
          ]);
          const craft = await craftRes.json();
          const people = await peopleRes.json();

          const craftBody = document.querySelector('#craft-table tbody');
          craftBody.innerHTML = '';
          (craft.rows || []).forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${row.craft}</td><td>${row.people_count}</td><td>${row.last_seen_at}</td>`;
            craftBody.appendChild(tr);
          });

          const peopleBody = document.querySelector('#people-table tbody');
          peopleBody.innerHTML = '';
          (people.rows || []).forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${row.name}</td><td>${row.craft}</td><td>${row._inserted_at}</td>`;
            peopleBody.appendChild(tr);
          });

          status.textContent = 'ok';
        } catch (err) {
          status.textContent = 'error';
        }
      }

      document.getElementById('refresh').addEventListener('click', loadData);
      loadData();
    </script>
  </body>
</html>
"""
