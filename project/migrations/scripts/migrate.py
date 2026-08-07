from project.migrations.scripts.router import init_router


if __name__ == "__main__":
    """Quality of life function to quickly execute migrations."""

    with init_router() as router:
        router.run()
