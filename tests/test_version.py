from project.version import __version__


def test_200_version(test_client):
    r = test_client.get("/")

    assert r.status_code == 200
    assert r.json() == {"version": __version__}
