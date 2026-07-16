"""Peewee migrations -- 001_init.py.

Some examples (model - class or model name)::

    > Model = migrator.orm['table_name']            # Return model in current state by name
    > Model = migrator.ModelClass                   # Return model in current state by name

    > migrator.sql(sql)                             # Run custom SQL
    > migrator.run(func, *args, **kwargs)           # Run python function with the given args
    > migrator.create_model(Model)                  # Create a model (could be used as decorator)
    > migrator.remove_model(model, cascade=True)    # Remove a model
    > migrator.add_fields(model, **fields)          # Add fields to a model
    > migrator.change_fields(model, **fields)       # Change fields
    > migrator.remove_fields(model, *field_names, cascade=True)
    > migrator.rename_field(model, old_field_name, new_field_name)
    > migrator.rename_table(model, new_table_name)
    > migrator.add_index(model, *col_names, unique=False)
    > migrator.add_not_null(model, *field_names)
    > migrator.add_default(model, field_name, default)
    > migrator.add_constraint(model, name, sql)
    > migrator.drop_index(model, *col_names)
    > migrator.drop_not_null(model, *field_names)
    > migrator.drop_constraints(model, *constraints)

"""

from contextlib import suppress

import peewee as pw
from peewee_migrate import Migrator


with suppress(ImportError):
    pass


def migrate(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your migrations here."""

    @migrator.create_model
    class Result(pw.Model):
        id = pw.AutoField()
        client_id = pw.CharField(max_length=255)
        object_id = pw.UUIDField()
        filename = pw.CharField(max_length=255)

        class Meta:
            table_name = "result"
            indexes = [(("client_id", "object_id"), True)]

    @migrator.create_model
    class Tag(pw.Model):
        id = pw.AutoField()
        tag_name = pw.CharField(max_length=255)
        project_id = pw.CharField(max_length=255)

        class Meta:
            table_name = "tag"
            indexes = [(("tag_name", "project_id"), True)]

    @migrator.create_model
    class TaggedResult(pw.Model):
        id = pw.AutoField()
        tag = pw.ForeignKeyField(column_name="tag_id", field="id", model=migrator.orm["tag"], on_delete="CASCADE")
        result = pw.ForeignKeyField(
            column_name="result_id", field="id", model=migrator.orm["result"], on_delete="CASCADE"
        )

        class Meta:
            table_name = "taggedresult"


def rollback(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your rollback migrations here."""

    migrator.remove_model("taggedresult")

    migrator.remove_model("tag")

    migrator.remove_model("result")
