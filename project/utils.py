from pathlib import Path

from project.models import PyProject


def get_project_root():
    return Path(__file__).parent.parent


def load_pyproject():
    import tomli

    with open(get_project_root() / "pyproject.toml", mode="rb") as f:
        pyproject_data = tomli.load(f)
        return PyProject(**pyproject_data)


def load_readme():
    with open(get_project_root() / "README.md", mode="r") as f:
        return f.read()
