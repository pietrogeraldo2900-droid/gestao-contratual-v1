from __future__ import annotations

from config.settings import load_settings
from app.routes.web_app import create_app

settings = load_settings()
app = create_app(settings=settings)
application = app
