"""Application factory for the OLAP práctica web UI."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from flask import Flask

# Ensure project root is importable when running ``python app/main.py`` directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.utils import get_logger, load_settings  # noqa: E402  (import after path tweak)

from .routes import bp as web_blueprint


def create_app() -> Flask:
    """Create and configure the Flask application."""

    settings = load_settings()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "olap-practica-secret")
    app.config["SETTINGS"] = settings
    app.register_blueprint(web_blueprint)
    get_logger().info("Aplicación Flask inicializada")

    return app


if __name__ == "__main__":
    settings = load_settings()
    flask_app = create_app()
    flask_app.run(host=settings.app["host"], port=settings.app["port"], debug=settings.app["debug"])
