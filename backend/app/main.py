import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import *  # noqa
from app.database import engine, Base, SessionLocal
from app.models import *  # noqa
from app.seed import seed_database
from app.routers import auth, admin, bulletins, links


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

app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(bulletins.router)
app.include_router(links.router)

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
