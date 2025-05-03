from ingestions.mock import OfflineDataLoader
import pandas
import asyncio
def test_assert():
    loader = OfflineDataLoader("./path")
    filename = asyncio.run(loader.download(2024, 12))
    df = pandas.read_parquet(filename)
    assert True
