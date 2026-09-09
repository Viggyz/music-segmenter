import aiofiles
import aiofiles.os
from pathlib import Path
import logging

async def make_folder(folder_path: str | Path) -> None:
    folder_path = Path(folder_path)
    if not await aiofiles.os.path.exists(folder_path):
        await aiofiles.os.makedirs(folder_path, exist_ok=True)
        logging.info("%s created successfully", folder_path)
    else:
        logging.info("%s already exists", folder_path)