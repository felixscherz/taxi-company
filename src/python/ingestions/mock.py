from pathlib import Path

import vcr
from ingestions import DataLoader

filepath = __file__
filedir = Path(__file__).parent


class OfflineDataLoader(DataLoader):
    async def download(self, year: int, month: int) -> str:
        with vcr.use_cassette(filedir / "tests/cassettes/test_ingestions/test_imports.yaml"):
            return await super().download(year, month)
