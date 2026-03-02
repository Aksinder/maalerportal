"""
Målerportal API Explorer – Web Server
======================================
Kör API-utforskning och visar resultaten som HTML i browsern.

Kör med:
    python scripts/explore_server.py
Öppna sedan: http://localhost:8765
"""
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from aiohttp import web

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

AUTH_BASE_URL = "https://api.gateway.meterportal.eu/v1/auth"
ME_BASE_URL   = "https://api.gateway.meterportal.eu/v1/me"
SMARTHOME_BASE_URL = "https://api.gateway.meterportal.eu/v1/smarthome"

PORT = 8765


# ── HTML template ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="sv">
<head>
<meta charset="utf-8">
<title>Målerportal API Explorer</title>
<style>
  body {{ font-family: system-ui, sans-serif; background:#111; color:#e0e0e0; margin:0; padding:24px; }}
  h1   {{ color:#f472b6; margin-bottom:4px; }}
  p.sub {{ color:#888; margin-top:0; margin-bottom:32px; font-size:.9em; }}
  .section {{ background:#1a1a1a; border:1px solid #333; border-radius:8px; margin-bottom:20px; overflow:hidden; }}
  .section-title {{ background:#222; padding:12px 16px; font-weight:600; font-size:.85em;
                    letter-spacing:.05em; color:#a78bfa; border-bottom:1px solid #333; }}
  .meta {{ padding:8px 16px; font-size:.8em; color:#888; border-bottom:1px solid #222; }}
  .badge {{ display:inline-block; padding:2px 8px; border-radius:4px; font-weight:700;
            font-size:.75em; margin-right:6px; }}
  .get  {{ background:#064e3b; color:#6ee7b7; }}
  .post {{ background:#1e3a5f; color:#93c5fd; }}
  .s200,.s201 {{ background:#064e3b; color:#6ee7b7; }}
  .s404,.s500 {{ background:#7f1d1d; color:#fca5a5; }}
  .s429 {{ background:#78350f; color:#fcd34d; }}
  pre  {{ margin:0; padding:16px; overflow-x:auto; font-size:.8em; line-height:1.5;
          white-space:pre-wrap; word-break:break-word; }}
  .error {{ color:#fca5a5; }}
  a.reload {{ display:inline-block; margin-bottom:24px; padding:8px 18px; background:#f472b6;
              color:#000; border-radius:6px; text-decoration:none; font-weight:600; }}
  a.reload:hover {{ background:#ec4899; }}
</style>
</head>
<body>
<h1>Målerportal API Explorer</h1>
<p class="sub">Uppdaterad: {timestamp}</p>
<a class="reload" href="/">↻ Kör igen</a>
{sections}
</body>
</html>"""

SECTION_TEMPLATE = """<div class="section">
  <div class="section-title">{title}</div>
  <div class="meta">
    <span class="badge {method_class}">{method}</span>{url}
    {body_html}
    &nbsp;→&nbsp;<span class="badge {status_class}">HTTP {status}</span>
  </div>
  <pre>{body}</pre>
</div>"""


def status_class(status: int) -> str:
    if status in (200, 201, 202):
        return f"s{status}"
    if status == 404:
        return "s404"
    if status == 429:
        return "s429"
    return "s500"


def render_section(title, method, url, status, data, req_body=None):
    method_class = method.lower()
    body_html = ""
    if req_body:
        body_html = f'<br><small>Body: <code>{json.dumps(req_body)}</code></small>'

    # Mask sensitive fields
    display = dict(data) if isinstance(data, dict) else data
    if isinstance(display, dict):
        if "token" in display:
            display = {**display, "token": display["token"][:20] + "…"}
        if "refreshToken" in display:
            display = {**display, "refreshToken": display["refreshToken"][:20] + "…"}
        if "apiKey" in display:
            display = {**display, "apiKey": display["apiKey"][:12] + "…"}

    pretty = json.dumps(display, indent=2, ensure_ascii=False, default=str)
    css = "error" if status >= 400 else ""
    return SECTION_TEMPLATE.format(
        title=title,
        method=method,
        method_class=method_class,
        url=url,
        body_html=body_html,
        status=status,
        status_class=status_class(status),
        body=f'<span class="{css}">{pretty}</span>' if css else pretty,
    )


# ── API calls ──────────────────────────────────────────────────────────────────

async def run_exploration(email: str, password: str) -> str:
    sections = []

    async with aiohttp.ClientSession() as session:

        # 1. check-auth-methods
        async with session.post(
            f"{AUTH_BASE_URL}/check-auth-methods",
            json={"emailAddress": email},
            headers={"Content-Type": "application/json"},
        ) as r:
            d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
            sections.append(render_section(
                "1. POST /v1/auth/check-auth-methods",
                "POST", f"{AUTH_BASE_URL}/check-auth-methods", r.status, d,
            ))
            auth_ok = r.ok

        if not auth_ok:
            return HTML_TEMPLATE.format(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sections="".join(sections),
            )

        # 2. login
        login_payload = {"emailAddress": email, "password": password, "platform": "smarthome"}
        async with session.post(
            f"{AUTH_BASE_URL}/login",
            json=login_payload,
            headers={"Content-Type": "application/json"},
        ) as r:
            d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
            sections.append(render_section(
                "2. POST /v1/auth/login",
                "POST", f"{AUTH_BASE_URL}/login", r.status, d,
            ))
            jwt_token = d.get("token") if isinstance(d, dict) else None

        if not jwt_token:
            return HTML_TEMPLATE.format(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sections="".join(sections),
            )

        # 3. smarthome-apikey
        async with session.post(
            f"{ME_BASE_URL}/smarthome-apikey",
            json={"description": "API Explorer Script"},
            headers={"Authorization": f"Bearer {jwt_token}", "Content-Type": "application/json"},
        ) as r:
            d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
            sections.append(render_section(
                "3. POST /v1/me/smarthome-apikey",
                "POST", f"{ME_BASE_URL}/smarthome-apikey", r.status, d,
            ))
            api_key = d.get("apiKey") if isinstance(d, dict) else None

        if not api_key:
            return HTML_TEMPLATE.format(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sections="".join(sections),
            )

        # 4. addresses
        async with session.get(
            f"{SMARTHOME_BASE_URL}/addresses",
            headers={"ApiKey": api_key},
        ) as r:
            addresses = await r.json() if r.content_type == "application/json" else []
            sections.append(render_section(
                "4. GET /v1/smarthome/addresses",
                "GET", f"{SMARTHOME_BASE_URL}/addresses", r.status, addresses,
            ))

        # collect installations
        installations = []
        if isinstance(addresses, list):
            for addr in addresses:
                addr_str = addr.get("address", "?")
                for inst in addr.get("installations", []):
                    installations.append({
                        "id": inst.get("installationId"),
                        "label": f"{addr_str} – {inst.get('meterSerial','')} [{inst.get('installationType','')}]",
                    })

        # 5+6+7. Per installation
        now = datetime.now(timezone.utc)
        from_date = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")
        to_date   = now.strftime("%Y-%m-%dT23:59:59Z")

        for inst in installations:
            iid   = inst["id"]
            label = inst["label"]

            # 5. latest
            async with session.get(
                f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/latest",
                headers={"ApiKey": api_key},
            ) as r:
                d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
                sections.append(render_section(
                    f"5. GET …/installations/{iid}/readings/latest<br><small style='color:#888'>{label}</small>",
                    "GET",
                    f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/latest",
                    r.status, d,
                ))

            # 6. historical – senaste 30 dagarna
            body = {"from": from_date, "to": to_date}
            async with session.post(
                f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/historical",
                json=body,
                headers={"ApiKey": api_key, "Content-Type": "application/json"},
            ) as r:
                d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
                sections.append(render_section(
                    f"6. POST …/installations/{iid}/readings/historical (senaste 30 dagar)<br><small style='color:#888'>{label}</small>",
                    "POST",
                    f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/historical",
                    r.status, d, req_body=body,
                ))

            # 7. historical – full range i 31-dagars-chunk (max per anrop)
            all_readings = []
            chunk_results = []
            earliest = datetime(2025, 5, 26, tzinfo=timezone.utc)
            chunk_end = now
            chunk_num = 0

            while chunk_end > earliest:
                chunk_start = max(chunk_end - timedelta(days=31), earliest)
                from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                to_str   = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                req_body_chunk = {"from": from_str, "to": to_str}

                async with session.post(
                    f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/historical",
                    json=req_body_chunk,
                    headers={"ApiKey": api_key, "Content-Type": "application/json"},
                ) as r:
                    d = await r.json() if r.content_type == "application/json" else {"raw": await r.text()}
                    readings = d.get("readings", []) if isinstance(d, dict) else []
                    all_readings.extend(readings)
                    chunk_results.append({
                        "chunk": chunk_num + 1,
                        "from": from_str,
                        "to": to_str,
                        "status": r.status,
                        "count": len(readings),
                    })

                chunk_end = chunk_start - timedelta(seconds=1)
                chunk_num += 1

            # Sort all readings by timestamp
            all_readings.sort(key=lambda x: x.get("timestamp", ""))

            combined = {
                "installationId": iid,
                "totalReadings": len(all_readings),
                "chunks": chunk_results,
                "readings": all_readings,
            }
            sections.append(render_section(
                f"7. POST …/installations/{iid}/readings/historical "
                f"(full range, {len(all_readings)} avläsningar i {chunk_num} anrop)<br>"
                f"<small style='color:#888'>{label} · 2025-05-26 → idag</small>",
                "POST",
                f"{SMARTHOME_BASE_URL}/installations/{iid}/readings/historical",
                200,
                combined,
                req_body={"note": f"{chunk_num} chunk-anrop à max 31 dagar"},
            ))

    return HTML_TEMPLATE.format(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        sections="".join(sections),
    )


# ── Web server ─────────────────────────────────────────────────────────────────

async def handle(request: web.Request) -> web.Response:
    email    = os.environ.get("EMAIL", "")
    password = os.environ.get("PASSWORD", "")

    if not email or not password or email == "din@epost.se":
        return web.Response(
            text="<h1>Saknar credentials</h1><p>Fyll i EMAIL och PASSWORD i .env</p>",
            content_type="text/html",
        )

    html = await run_exploration(email, password)
    return web.Response(text=html, content_type="text/html")


def main():
    app = web.Application()
    app.router.add_get("/", handle)
    print(f"API Explorer körs på http://localhost:{PORT}")
    web.run_app(app, host="127.0.0.1", port=PORT, print=None)


if __name__ == "__main__":
    main()
