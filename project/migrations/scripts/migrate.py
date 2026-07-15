from project.migrations.scripts.router import router


if __name__ == "__main__":
    """Quality of life function to quickly execute migrations."""

    router.run()
