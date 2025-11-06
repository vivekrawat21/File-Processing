# app/storage/local_storage.py
import csv, aiofiles, orjson
from typing import AsyncGenerator

async def open_local_file(path: str, batch_size: int) -> AsyncGenerator[dict, None]:
    """Async generator that yilds rows in batches of batch_size."""
    if path.endswith(".csv"):
        async with aiofiles.open(path, "r") as f:
            reader = csv.DictReader(await f.readlines())
            batch, index = [], 0
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield {"index": index, "rows": batch}
                    batch, index = [], index + 1
            if batch:
                yield {"index": index, "rows": batch}
    elif path.endswith(".json"):
        async with aiofiles.open(path, "r") as f:
            content = await f.read()
            data = orjson.loads(content)
            for i in range(0, len(data), batch_size):
                yield {"index": i // batch_size, "rows": data[i:i+batch_size]}

    else:
        raise ValueError("Unsupported file type")