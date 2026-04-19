"""FastAPI dashboard for Phase 1 live view.

Two routes:
  GET /           full page shell (served once per browser tab)
  GET /fragment   the live-data partial (HTMX refreshes this every 15s)

Run locally:
  .venv/bin/uvicorn polysport.dashboard.app:app --reload
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from supabase import create_client

from polysport.dashboard.data import get_live_state

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

app = FastAPI(title="PolySport Live")


def _sb():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return _TEMPLATES.TemplateResponse(request, "index.html")


@app.get("/fragment", response_class=HTMLResponse)
def fragment(request: Request):
    state = get_live_state(_sb())
    return _TEMPLATES.TemplateResponse(
        request, "fragment.html", {"state": state})
