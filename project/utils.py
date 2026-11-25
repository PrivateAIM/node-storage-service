from pathlib import Path
import typing as t

from flame_hub import CoreClient
from flame_hub.auth import StaticAuth
from flame_hub.models import AnalysisBucket, StorageCapabilityTokenRequest, StorageCapability

from project.models import PyProject


def get_project_root():
    return Path(__file__).parent.parent


def load_pyproject():
    import tomli

    with open(get_project_root() / "pyproject.toml", mode="rb") as f:
        pyproject_data = tomli.load(f)
        return PyProject(**pyproject_data)


def load_readme():
    with open(get_project_root() / "README.md", mode="r") as f:
        return f.read()


def get_storage_cap_auth(
    core_client: CoreClient,
    analysis_id: str,
    bucket: AnalysisBucket,
    action: t.Literal["read", "write", "delete"],
) -> StaticAuth:
    token = core_client.mint_storage_cap_token(
        analysis_id=analysis_id,
        capability_request=StorageCapabilityTokenRequest(
            capabilities=[
                StorageCapability(
                    resource_type="bucket",
                    resource_id=str(bucket.bucket_id),
                    actions=[action],
                ),
            ],
            lifetime=3_600,
        ),
    )

    return StaticAuth(access_token=token.token)
