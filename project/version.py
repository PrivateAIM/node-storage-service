from project.utils import load_pyproject


__version__ = load_pyproject().project.version
__version_info__ = tuple(int(token) for token in __version__.split("."))
