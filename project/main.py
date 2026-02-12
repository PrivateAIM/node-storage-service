import json
import sys

import uvicorn

from project.server import get_server_instance

app = get_server_instance()


def run_server():
    uvicorn.run(app, host="0.0.0.0", port=8000)


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
