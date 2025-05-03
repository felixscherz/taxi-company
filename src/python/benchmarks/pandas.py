import pandas
from ingestions import DataLoader
import asyncio


def main():

    loader = DataLoader("./data")
    filepath = asyncio.run(loader.download(2024, 12))


    pandas.read_parquet(filepath)
