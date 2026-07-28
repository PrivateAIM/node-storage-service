import io

from httpx2 import Response


def detail_of(r: Response) -> str:
    """Retrieve the content of the `detail` key within an error response."""
    return r.json()["detail"]


def wrap_bytes_for_request(
    b: bytes,
    file_name: str = "upload.bin",
    content_type: str = "application/octet-stream",
):
    """Wrap a bytes object into a dictionary s.t. it can be passed into a httpx2 request."""
    return {"file": (file_name, io.BytesIO(b), content_type)}
