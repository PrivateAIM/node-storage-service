import os
import json
import sys
import pathlib

import uvicorn

from project.server import get_server_instance, get_project_root

app = get_server_instance()


def config_server(host: str = "0.0.0.0", port: int = 8000):
    os.makedirs(get_project_root() / "logs", exist_ok=True)
    log_config_file_path = get_project_root() / "config" / "logging.json"

    with open(log_config_file_path) as f:
        log_config = json.load(f)

    filename = log_config["handlers"]["file_handler"]["filename"]
    log_config["handlers"]["file_handler"]["filename"] = get_project_root() / "logs" / filename

    config = uvicorn.Config(app, host=host, port=port, log_config=log_config)

    return uvicorn.Server(config)


def run_server():  # pragma: no cover
    config_server().run()


def openapi_spec():
    filename = sys.argv[1]

    if pathlib.Path(filename).suffix != ".json":
        raise ValueError(f"File {filename} needs to be a json file.")

    spec = app.openapi()
    with open(filename, "w") as f:
        json.dump(spec, f)


if __name__ == "__main__":  # pragma: no cover
    run_server()
