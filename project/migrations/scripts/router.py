import json
import os
import logging.config

from peewee_migrate import Router

from project.dependencies import get_postgres_db, get_settings
from project.server import get_project_root


def init_router() -> Router:
    os.makedirs(get_project_root() / "logs", exist_ok=True)

    # peewee-migrate's logger has no handler configured per default.
    with open(get_project_root() / "config" / "logging.json") as f:
        config = json.load(f)
        logging.config.dictConfig(config)

    return Router(
        get_postgres_db(get_settings()),
        migrate_dir=get_project_root() / "project" / "migrations",
        migrate_table=get_settings().postgres.migrations_tablename,
        # Ignore the BaseModel from crud.py.
        ignore=("basemodel",),
    )
