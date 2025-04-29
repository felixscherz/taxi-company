import asyncio

import pytest
import pytest_recording
from ingestions import DataLoader


def test_ingestions():
    assert True


@pytest.mark.vcr("taxidata.yaml")
def test_imports():
    import ingestions

    loader = DataLoader("./path")
    asyncio.run(loader.download(2024, 12))


def test_httpx():
    import httpx
