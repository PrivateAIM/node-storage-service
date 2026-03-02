from functools import cached_property
import logging

import peewee as pw
import playhouse.shortcuts

from project.dependencies import get_settings, get_postgres_db


logger = logging.getLogger(__name__)
proxy = pw.DatabaseProxy()


class BaseModel(pw.Model):
    class Meta:
        database = proxy
        model_metadata_class = playhouse.shortcuts.ThreadSafeDatabaseMetadata


class Tag(BaseModel):
    tag_name = pw.CharField(null=False)
    project_id = pw.CharField(null=False)

    class Meta:
        indexes = ((("tag_name", "project_id"), True),)


class Result(BaseModel):
    client_id = pw.CharField(null=False)
    object_id = pw.UUIDField(null=False)
    filename = pw.CharField(null=False)

    class Meta:
        indexes = ((("client_id", "object_id"), True),)


class TaggedResult(BaseModel):
    tag = pw.ForeignKeyField(Tag, null=False, on_delete="CASCADE")
    result = pw.ForeignKeyField(Result, null=False, on_delete="CASCADE")


class Postgres:
    def __init__(self):
        logger.info("Initializing connection to Postgres for storing tags and result metadata")

    @cached_property
    def db(self):
        return get_postgres_db(settings=get_settings())

    def test_connection(self):
        """Tests connection and binding of tables needed for event logging."""
        with self.db:
            pass

    def setup(self):
        """Initializes a configured database with the help of the database proxy that is already bound to the models."""
        if proxy.obj is not None:
            raise pw.PeeweeException("Database proxy is already initialized.")
        proxy.initialize(self.db)
        with self.db:
            # Create tables if they do not exist yet.
            self.db.create_tables((Tag, Result, TaggedResult))
        logger.info(f"Connected to database at port {get_settings().postgres.port} to store tags and results.")

    def teardown(self):
        """Closes all connections inside the pool. This is meant to be called during lifespan spin down."""
        self.db.close()


postgres: Postgres = Postgres()
