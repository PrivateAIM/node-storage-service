import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import flame_hub
import httpx2 as httpx
import pytest
from fastapi import HTTPException
from starlette import status

from project.dependencies import get_analysis_id

NODE_ID = uuid.uuid4()
PAGE_LIMIT = 50


def _analysis_node(client_id: uuid.UUID | None = None, analysis_id: uuid.UUID | None = None, with_analysis=True):
    analysis = SimpleNamespace(id=analysis_id or uuid.uuid4(), client_id=client_id) if with_analysis else None
    return SimpleNamespace(analysis=analysis)


def _core_client(*pages, analysis=None) -> MagicMock:
    core_client = MagicMock()
    core_client.find_analysis_nodes.side_effect = list(pages)
    core_client.get_analysis.return_value = analysis
    return core_client


def _expected_find_call(offset: int) -> dict:
    return {
        "filter": {"nodeId": str(NODE_ID)},
        "sort": {"by": "created_at", "order": "descending"},
        "page": {"limit": PAGE_LIMIT, "offset": offset},
    }


def test_found_on_first_page():
    client_id, analysis_id = uuid.uuid4(), uuid.uuid4()
    core_client = _core_client([_analysis_node(), _analysis_node(client_id, analysis_id)])

    assert get_analysis_id(str(client_id), core_client, NODE_ID) == str(analysis_id)
    core_client.find_analysis_nodes.assert_called_once_with(**_expected_find_call(0))
    core_client.get_analysis.assert_not_called()


def test_found_on_later_page():
    client_id, analysis_id = uuid.uuid4(), uuid.uuid4()
    core_client = _core_client(
        [_analysis_node() for _ in range(PAGE_LIMIT)],
        [_analysis_node(client_id, analysis_id)],
    )

    assert get_analysis_id(str(client_id), core_client, NODE_ID) == str(analysis_id)
    assert [c.kwargs for c in core_client.find_analysis_nodes.call_args_list] == [
        _expected_find_call(0),
        _expected_find_call(PAGE_LIMIT),
    ]


def test_analysis_node_without_analysis_is_skipped():
    client_id, analysis_id = uuid.uuid4(), uuid.uuid4()
    core_client = _core_client([_analysis_node(with_analysis=False), _analysis_node(client_id, analysis_id)])

    assert get_analysis_id(str(client_id), core_client, NODE_ID) == str(analysis_id)


def test_falls_back_to_claim_as_analysis_id():
    claim = str(uuid.uuid4())
    core_client = _core_client([_analysis_node()], analysis=SimpleNamespace(id=uuid.UUID(claim)))

    assert get_analysis_id(claim, core_client, NODE_ID) == claim
    core_client.get_analysis.assert_called_once_with(claim)


def test_no_match_is_forbidden():
    core_client = _core_client([_analysis_node()], analysis=None)

    with pytest.raises(HTTPException) as e:
        get_analysis_id(str(uuid.uuid4()), core_client, NODE_ID)

    assert e.value.status_code == status.HTTP_403_FORBIDDEN


def test_non_uuid_claim_is_forbidden_without_listing_analysis_nodes():
    core_client = _core_client(analysis=None)

    with pytest.raises(HTTPException) as e:
        get_analysis_id("not-a-uuid", core_client, NODE_ID)

    assert e.value.status_code == status.HTTP_403_FORBIDDEN
    core_client.find_analysis_nodes.assert_not_called()


def test_hub_error_is_bad_gateway():
    core_client = MagicMock()
    core_client.find_analysis_nodes.side_effect = flame_hub.HubAPIError("boom", httpx.Request("GET", "http://hub"))

    with pytest.raises(HTTPException) as e:
        get_analysis_id(str(uuid.uuid4()), core_client, NODE_ID)

    assert e.value.status_code == status.HTTP_502_BAD_GATEWAY
