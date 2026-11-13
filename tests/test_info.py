from project.version import __version__


def test_200_info(test_client):
    r = test_client.get("/")

    assert r.status_code == 200

    d = r.json()

    assert d["version"] == __version__
    assert isinstance(d["uptime"], int)
    assert d["uptime"] > 0
