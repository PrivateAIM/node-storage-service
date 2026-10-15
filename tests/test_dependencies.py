import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import flame_hub
import httpx2 as httpx
import pytest
from fastapi import HTTPException
from starlette import status

from project.dependencies import get_analysis_id


def _core_client(analyses=(), analysis=None) -> MagicMock:
    core_client = MagicMock()
    core_client.find_analyses.return_value = list(analyses)
    core_client.get_analysis.return_value = analysis
    return core_client


def test_found_by_client_id():
    client_id, analysis_id = uuid.uuid4(), uuid.uuid4()
    core_client = _core_client([SimpleNamespace(id=analysis_id)])

    assert get_analysis_id(str(client_id), core_client) == str(analysis_id)
    core_client.find_analyses.assert_called_once_with(filter={"clientId": str(client_id)}, page={"limit": 1})
    core_client.get_analysis.assert_not_called()


def test_falls_back_to_claim_as_analysis_id():
    # TODO(remove): goes away with the Keycloak-era fallback in get_analysis_id.
    claim = str(uuid.uuid4())
    core_client = _core_client(analysis=SimpleNamespace(id=uuid.UUID(claim)))

    assert get_analysis_id(claim, core_client) == claim
    core_client.get_analysis.assert_called_once_with(claim)


def test_no_match_is_forbidden():
    core_client = _core_client()

    with pytest.raises(HTTPException) as e:
        get_analysis_id(str(uuid.uuid4()), core_client)

    assert e.value.status_code == status.HTTP_403_FORBIDDEN
    assert e.value.detail == "Token is not associated with an analysis"
    core_client.get_analysis.assert_called_once()


def test_non_uuid_claim_is_forbidden_without_hub_calls():
    core_client = _core_client()

    with pytest.raises(HTTPException) as e:
        get_analysis_id("not-a-uuid", core_client)

    assert e.value.status_code == status.HTTP_403_FORBIDDEN
    assert e.value.detail == "Token is not associated with an analysis"
    core_client.find_analyses.assert_not_called()
    core_client.get_analysis.assert_not_called()


def test_hub_error_is_bad_gateway():
    core_client = MagicMock()
    core_client.find_analyses.side_effect = flame_hub.HubAPIError("boom", httpx.Request("GET", "http://hub"))

    with pytest.raises(HTTPException) as e:
        get_analysis_id(str(uuid.uuid4()), core_client)

    assert e.value.status_code == status.HTTP_502_BAD_GATEWAY
