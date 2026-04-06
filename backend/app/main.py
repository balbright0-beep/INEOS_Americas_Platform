import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from collections import defaultdict

from app.config import *  # noqa
from app.database import engine, Base, SessionLocal
from app.models import *  # noqa
from app.seed import seed_database
from app.routers import auth, admin, bulletins, links, data


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        seed_database(db)
    finally:
        db.close()
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
    # Disable caching on HTML to prevent stale pages
    if request.url.path in ("/", "/index.html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(bulletins.router)
app.include_router(links.router)
app.include_router(data.router)

# Serve generated Dashboard HTML
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the latest generated Americas Dashboard."""
    dash_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'Americas_Daily_Dashboard.html')
    if os.path.exists(dash_path):
        with open(dash_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<html><body><h1>Dashboard not generated yet</h1><p>Go to Data Upload and click 'Rebuild All Dashboards' after uploading source files.</p></body></html>")

from fastapi.responses import HTMLResponse

# Public last-update endpoint (no auth required)
from app.database import get_db
from app.models import AppState
from fastapi import Depends
from sqlalchemy.orm import Session

@app.get("/api/last-update")
def public_last_update(db: Session = Depends(get_db)):
    state = db.query(AppState).filter(AppState.key == "last_master_upload").first()
    return {"last_update": state.value if state else None}

static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
