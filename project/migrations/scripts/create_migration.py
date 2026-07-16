import sys

from project import crud
from project.migrations.scripts.router import init_router


if __name__ == "__main__":
    """Quality of life function to quickly create a new migration. As of now, it is not possible to ignore specific
    models via the pw_migrate CLI."""

    router = init_router()
    router.create(sys.argv[1], auto=crud)
