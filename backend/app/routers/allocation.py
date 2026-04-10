"""Allocation API — serves vehicle + dealer data for the Allocation tool.

The Allocation tool (separate Render app) fetches this endpoint to get the
same DEALERS / V_DATA / V_DICT / PIPELINE_COMP / SELL_THROUGH / DAYS_TO_SELL
/ PLANT_AFFINITY data that was previously only available by uploading the
encrypted Master File.

GET /api/allocation/data  → JSON with all 7 data structures + DATA_TS
"""

from __future__ import annotations

import os
import sys
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api/allocation", tags=["allocation"])


@router.get('/data')
def get_allocation_data(user=Depends(get_current_user), db: Session = Depends(get_db)):
    """Return the full allocation dataset computed from uploaded source files.

    This replaces the Master File → allocation_app.py → HTML injection pipeline.
    The Allocation tool's JavaScript calls this endpoint on load and populates
    V_DATA, V_DICT, DEALERS, etc. from the response.
    """
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    cache_dir = os.path.join(backend_dir, 'cache')
    data_dir = os.path.join(cache_dir, 'data')

    if not os.path.exists(os.path.join(data_dir, 'sap_export.parquet')):
        raise HTTPException(404, 'SAP Vehicle Export has not been uploaded yet.')

    from data_hub.allocation_data import compute_allocation_data
    try:
        result = compute_allocation_data(cache_dir)
    except Exception as e:
        raise HTTPException(500, f'Failed to compute allocation data: {e}')

    return JSONResponse(
        content=result,
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
        },
    )
