from pathlib import Path

import httpx


class DownloadFailed(Exception): ...


BASE_URL = "https://d37ci6vzurychx.cloudfront.net"


PATH_TEMPLATE = "/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"


class DataLoader:
    def __init__(self, path: str):
        self.client = httpx.AsyncClient(base_url=BASE_URL)
        self.path = path

    async def download(self, year: int, month: int) -> str:
        path = PATH_TEMPLATE.format(year=year, month=month)
        response = await self.client.get(path)
        if not response.is_success:
            raise DownloadFailed(
                f"Download for {year=}, {month=} failed with {response.status_code} {response.text}"
            )
        save_path = Path(self.path)
        if not save_path.exists():
            save_path.mkdir(parents=True)

        with open(save_path / f"data-{year:04d}-{month:02d}.parquet", "wb") as handle:
            filename = handle.name
            async for chunk in response.aiter_bytes():
                handle.write(chunk)

            return filename
