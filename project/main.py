import os
import json
import logging.config
import sys

import uvicorn

from project.server import get_server_instance, get_project_root

app = get_server_instance()


def run_server():
    os.makedirs("logs", exist_ok=True)
    log_config_file_path = get_project_root() / "config" / "logging.json"

    with open(log_config_file_path) as f:
        log_config = json.load(f)

    logging.config.dictConfig(log_config)

    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=log_config)


def openapi_spec(filename: str = "openapi.json"):
    try:
        filename = sys.argv[1]
    except IndexError:
        pass
    spec = app.openapi()
    with open(filename, "w") as f:
        json.dump(spec, f)


if __name__ == "__main__":
    run_server()
