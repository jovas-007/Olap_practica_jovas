"""Application factory for the OLAP práctica web UI."""
from __future__ import annotations

import os

from flask import Flask

from etl.utils import get_logger, load_settings

from .routes import bp as web_blueprint


def create_app() -> Flask:
    """Create and configure the Flask application."""

    settings = load_settings()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "olap-practica-secret")
    app.config["SETTINGS"] = settings
    app.register_blueprint(web_blueprint)

    @app.before_first_request
    def _log_startup() -> None:  # pragma: no cover - logging side effect
        get_logger().info("Aplicación Flask inicializada")

    return app


if __name__ == "__main__":
    settings = load_settings()
    flask_app = create_app()
    flask_app.run(host=settings.app["host"], port=settings.app["port"], debug=settings.app["debug"])
