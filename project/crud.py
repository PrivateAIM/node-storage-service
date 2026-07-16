import peewee as pw
import playhouse.shortcuts


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
