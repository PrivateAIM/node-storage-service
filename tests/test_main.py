import pathlib

import pytest

from project.main import openapi_spec
from tests.common.helpers import next_random_string


def test_openapi_spec(monkeypatch, tmp_path):
    filename = f"{tmp_path}/{next_random_string()}.json"
    monkeypatch.setattr("sys.argv", [None, filename])
    openapi_spec()

    assert pathlib.Path(filename).exists()


def test_openapi_spec_wrong_file(monkeypatch):
    filename = next_random_string()
    monkeypatch.setattr("sys.argv", [None, filename])

    with pytest.raises(ValueError) as e:
        openapi_spec()

    assert str(e.value) == f"File {filename} needs to be a json file."
