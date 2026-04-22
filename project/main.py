import os
import json
import sys
import pathlib

import uvicorn

from project.server import get_server_instance, get_project_root

app = get_server_instance()


def run_server():
    os.makedirs("logs", exist_ok=True)
    log_config_file_path = get_project_root() / "config" / "logging.json"

    with open(log_config_file_path) as f:
        log_config = json.load(f)

    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=log_config)


def openapi_spec():
    filename = sys.argv[1]

    if pathlib.Path(filename).suffix != ".json":
        raise ValueError(f"File {filename} needs to be a json file.")

    spec = app.openapi()
    with open(filename, "w") as f:
        json.dump(spec, f)


if __name__ == "__main__":
    run_server()
