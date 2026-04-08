import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from collections import defaultdict
from sqlalchemy.orm import Session

from app.config import *  # noqa
from app.database import engine, Base, SessionLocal, get_db
from app.models import *  # noqa
from app.seed import seed_database
from app.routers import auth, admin, bulletins, links, data


def _restore_cache_on_startup():
    """Restore source parquets from PostgreSQL — DO NOT rebuild the dashboard.

    Render's container filesystem is wiped on every deploy, so the parquets
    under backend/cache/data/ disappear. We restore them from the CachedFile
    table so the next manual rebuild has the latest uploaded sources to
    work with.

    IMPORTANT: We intentionally DO NOT auto-call rebuild_dashboard() here.
    The rebuild path through master_assembler+processor_original is currently
    producing inflated MTD totals (~400 vs the correct 29) compared to the
    upload-time generation, and auto-running it on every deploy was
    overwriting the user's known-good dashboard with the bad rebuild.
    Until the rebuild discrepancy is fixed, only manual rebuilds should
    be allowed to write Americas_Daily_Dashboard.html.
    """
    try:
        import sys
        backend_dir = os.path.dirname(os.path.dirname(__file__))
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)

        from data_hub.orchestrator import DataHub

        hub = DataHub(
            cache_dir=os.path.join(backend_dir, 'cache'),
            ref_db_path=os.path.join(backend_dir, 'reference', 'reference.db'),
            template_path=os.path.join(backend_dir, 'templates', 'dashboard_template.html'),
            output_dir=os.path.join(backend_dir, 'outputs'),
        )

        restored = hub.restore_all_from_db()
        print(f"[startup] Restored {restored} cached files from DB (no rebuild)")
    except Exception as e:
        print(f"[startup] Cache restore failed (non-fatal): {e}")
        import traceback
        traceback.print_exc()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        seed_database(db)
    finally:
        db.close()
    # Restore cached source files from DB. Does NOT auto-rebuild the
    # dashboard — see _restore_cache_on_startup for why.
    _restore_cache_on_startup()
    yield


app = FastAPI(title="INEOS Americas Platform", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Simple rate limiting (100 requests per minute per IP)
_rate_limit = defaultdict(list)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        ip = request.client.host
        now = time.time()
        _rate_limit[ip] = [t for t in _rate_limit[ip] if now - t < 60]
        if len(_rate_limit[ip]) > 100:
            return Response("Rate limit exceeded", status_code=429)
        _rate_limit[ip].append(now)
    response = await call_next(request)
    if request.url.path in ("/", "/index.html", "/dashboard"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

# API routers
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(bulletins.router)
app.include_router(links.router)
app.include_router(data.router)

# Lightweight health check for Render zero-downtime deploys
# Returns 200 immediately with no DB or filesystem dependencies so the
# load balancer can confirm the new container is up before cutting traffic.
@app.get("/health")
def health():
    return {"status": "ok"}

# Public last-update endpoint (no auth required)
@app.get("/api/last-update")
def public_last_update(db: Session = Depends(get_db)):
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    return {"last_update": state.value if state else None}

# Diagnostic endpoint to check dashboard file status
@app.get("/api/dashboard-status")
def dashboard_status():
    """Check if the dashboard output file exists and when it was last modified."""
    base = os.path.dirname(os.path.dirname(__file__))
    output_path = os.path.join(base, 'outputs', 'Americas_Daily_Dashboard.html')
    template_path = os.path.join(base, 'templates', 'dashboard_template.html')
    cache_dir = os.path.join(base, 'cache', 'data')

    result = {
        'output_path': output_path,
        'output_exists': os.path.exists(output_path),
        'template_exists': os.path.exists(template_path),
        'cache_dir_exists': os.path.isdir(cache_dir),
        'base_dir': base,
        'cwd': os.getcwd(),
    }

    if result['output_exists']:
        stat = os.stat(output_path)
        from datetime import datetime
        result['output_size'] = stat.st_size
        result['output_modified'] = datetime.fromtimestamp(stat.st_mtime).isoformat()

    # Check Santander JSON files
    sant_info = {}
    for fname in ['santander.json', 'santander_finance.json', 'santander_lease.json']:
        fpath = os.path.join(cache_dir, fname)
        if os.path.exists(fpath):
            import json as _json
            try:
                with open(fpath) as f:
                    data = _json.load(f)
                monthly = data.get('monthly', {})
                daily = data.get('daily', {})
                sant_info[fname] = {
                    'size': os.path.getsize(fpath),
                    'keys': list(data.keys()),
                    'product': data.get('product', '?'),
                    'monthly_count': len(monthly),
                    'monthly_total': sum(int(v) for v in monthly.values() if isinstance(v, (int, float))),
                    'monthly_sample': dict(list(monthly.items())[-3:]) if monthly else {},
                    'daily_count': len(daily),
                    'daily_total': sum(int(v) for v in daily.values() if isinstance(v, (int, float))),
                    'daily_sample': dict(list(sorted(daily.items()))[-5:]) if daily else {},
                }
            except Exception as e:
                sant_info[fname] = {'error': str(e)}
    result['santander_files'] = sant_info

    # Also check santander_latest.json in cache root
    slj = os.path.join(base, 'cache', 'santander_latest.json')
    if os.path.exists(slj):
        result['santander_latest_size'] = os.path.getsize(slj)

    if result['template_exists']:
        result['template_size'] = os.path.getsize(template_path)

    if result['cache_dir_exists']:
        result['cached_files'] = os.listdir(cache_dir)
    else:
        result['cached_files'] = []

    return result

def _inject_margaret_key(html: str) -> str:
    """Inject the Anthropic API key for the Margaret in-dashboard chat assistant.

    The dashboard JS reads window.__MARGARET_KEY and calls the Anthropic API
    directly from the browser (with the anthropic-dangerous-direct-browser-access
    header). The key is held in the MARGARET_API_KEY env var on Render so it
    never lives in the repo. Without this injection Margaret returns 401.
    """
    margaret_key = os.environ.get("MARGARET_API_KEY", "").strip()
    if not margaret_key:
        return html
    # Escape any quote/backslash so we can safely embed inside a JS string literal
    safe_key = margaret_key.replace("\\", "\\\\").replace('"', '\\"')
    snippet = f'<head><script>window.__MARGARET_KEY="{safe_key}";</script>'
    if "<head>" in html:
        return html.replace("<head>", snippet, 1)
    return html

# Serve generated Dashboard HTML — MUST be before static mount
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the latest generated Americas Dashboard."""
    dash_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'Americas_Daily_Dashboard.html')
    if os.path.exists(dash_path):
        with open(dash_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=_inject_margaret_key(f.read()))

    # Fallback: try serving the template directly
    tmpl_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates', 'dashboard_template.html')
    if os.path.exists(tmpl_path):
        with open(tmpl_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=_inject_margaret_key(f.read()))
    return HTMLResponse(content="""<!DOCTYPE html><html><head>
    <link rel="stylesheet" href="/ids.css">
    <style>body{font-family:var(--ids-font-body);display:flex;align-items:center;justify-content:center;min-height:100vh;background:var(--ids-surface-page)}</style>
    </head><body><div style="text-align:center;max-width:400px">
    <h1 style="font-family:var(--ids-font-heading);font-size:24px;margin-bottom:12px">Dashboard Not Generated Yet</h1>
    <p style="color:var(--ids-text-secondary);font-size:14px">Upload source files in the Data Sources page, then click <strong>Rebuild All Dashboards</strong>.</p>
    <a href="/" style="display:inline-block;margin-top:20px;padding:10px 24px;background:var(--ids-accent);color:#fff;border-radius:4px;text-decoration:none;font-size:14px">Go to Platform</a>
    </div></body></html>""")

# Static files mount — LAST (catch-all for SPA)
static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
