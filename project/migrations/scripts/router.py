from contextlib import contextmanager
import json
import os
import logging.config
import typing as t

import peewee as pw
from peewee_migrate import Router

from project.config import Settings
from project.server import init_db
from project.utils import get_project_root


@contextmanager
def init_router() -> t.Iterator[Router]:
    os.makedirs(get_project_root() / "logs", exist_ok=True)

    # peewee-migrate's logger has no handler configured per default.
    with open(get_project_root() / "config" / "logging.json") as f:
        config = json.load(f)
        logging.config.dictConfig(config)

    s = Settings()
    db = pw.PostgresqlDatabase(
        s.postgres.db,
        user=s.postgres.user,
        password=s.postgres.password.get_secret_value(),
        host=s.postgres.host,
        port=s.postgres.port,
    )
    init_db(db)

    try:
        yield Router(
            db,
            migrate_dir=get_project_root() / "project" / "migrations",
            migrate_table=s.postgres.migrations_tablename,
            # Ignore the BaseModel from crud.py.
            ignore=("basemodel",),
        )
    finally:
        db.close()
