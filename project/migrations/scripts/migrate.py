from project.migrations.scripts.router import init_router


if __name__ == "__main__":
    """Quality of life function to quickly execute migrations."""

    router = init_router()
    router.run()
