import pathlib
import threading

import httpx
import peewee as pw
import pytest

from project import crud
from project.main import openapi_spec, config_server
from tests.common.helpers import next_random_string, eventually


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


def test_run_server(monkeypatch):
    host, port = "127.0.0.1", 8001
    server = config_server(host=host, port=port)

    # Reset proxy in case it was already initialized.
    monkeypatch.setattr(crud, "proxy", pw.DatabaseProxy())

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    def _wait_for_server_to_start():
        return server.started

    assert eventually(_wait_for_server_to_start)

    r = httpx.get(f"http://{host}:{port}/healthz")

    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

    server.should_exit = True
    thread.join(timeout=5)

    assert not thread.is_alive()
