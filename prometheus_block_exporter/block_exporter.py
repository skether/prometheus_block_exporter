import json
import logging
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ulid import ULID

from .block_copier import BlockCopier


LOGGER = logging.getLogger(__name__)


class Block():
    def __init__(self, path):
        self.path = path
        self.ulid = ULID.from_str(path.name)

    def __eq__(self, other):
        if isinstance(other, Block):
            return self.ulid == other.ulid and self.path == other.path
        elif isinstance(other, ULID):
            return self.ulid == other
        elif isinstance(other, str):
            return str(self.ulid) == other
        return False

    def __repr__(self):
        return f'Block(ulid: {repr(self.ulid)}, path: {repr(self.path)})'

    def __str__(self):
        return f'Block({str(self.ulid)} @ {str(self.path)})'


def iterate_blocks(path):
    ulid_filter = re.compile(r'^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}$')
    return (Block(block) for block in path.iterdir() if block.is_dir() and ulid_filter.match(block.name))


def main(prometheus_data_dir, export_data_dir, minimum_age_hours):
    export_data_dir.mkdir(parents=True, exist_ok=True)

    # Scan the data directories for Blocks
    prometheus_blocks = sorted(list(iterate_blocks(prometheus_data_dir)), key=lambda block: str(block.ulid))
    exported_blocks = sorted(list(iterate_blocks(export_data_dir)), key=lambda block: str(block.ulid))

    minimum_creation_time = datetime.now(tz=timezone.utc) - timedelta(hours=minimum_age_hours)

    # Load the status file if present
    status_file_path = export_data_dir.joinpath("block.exporter.json")
    status = {}
    if status_file_path.exists():
        LOGGER.info("Loading status file!")
        with open(status_file_path, 'r') as f:
            status = json.load(f)

    # Check if the export data directories content is consistent with the status file.
    LOGGER.info("Running consistency checks.")
    refresh_exported_blocks = False
    for key, value in list(status.items()):
        if key in exported_blocks:
            if value.get('successful', False):
                # Block was successfully exported and is still availiable.
                pass
            else:
                # Block was partially exported in the past. If possible will try to reexport, leave it otherwise.
                if key in prometheus_blocks:
                    shutil.rmtree(export_data_dir.joinpath(key), ignore_errors=True)
                    refresh_exported_blocks = True
                    LOGGER.warning(f"Block({key}) was partially exported. Removing to force retry!")
                else:
                    LOGGER.warning(f"Block({key}) was partially exported and is no longer available to retry.")
        else:
            if value.get('successful', False):
                # Block was exported successfully in the past, but since has been removed.
                LOGGER.info(f"Block({key}) was exported successfully but it is now missing.")
            else:
                # Block was not exported successfully, but is in the status file. This should not occour under normal circumstances.
                LOGGER.warning(f"Missing Block({key}) was not exported successfully in the past. Cleaning from the status file.")
                del status[key]
    for block in exported_blocks:
        if str(block.ulid) not in status:
            # Block was exported, but there's no record of it in the status file. Reexport if possible.
            if block.ulid in prometheus_blocks:
                shutil.rmtree(export_data_dir.joinpath(str(block.ulid)), ignore_errors=True)
                refresh_exported_blocks = True
                LOGGER.info(f"Block({str(block.ulid)}) was exported in the past, but there's no record of it. Will try to reexport.")
            else:
                LOGGER.info(f"Block({str(block.ulid)}) was exported in the past, but there's no record of it. It is no longer available, won't try to reexport.")
    if refresh_exported_blocks:
        exported_blocks = sorted(list(iterate_blocks(export_data_dir)), key=lambda block: str(block.ulid))

    block_copier = BlockCopier(target_directory=export_data_dir)
    block_copier.hash_dictionary = status

    # Loop through all blocks and copy the ones that were not copied yet and also old enough.
    LOGGER.info("Exporting blocks.")
    for block in prometheus_blocks:
        if block.ulid in exported_blocks:
            LOGGER.info(f"Skip: {str(block.ulid)} is already exported.")
            continue

        if str(block.ulid) in status and status[str(block.ulid)].get('successful', False):
            LOGGER.info(f'Skip: {str(block.ulid)} was exported successfully in the past.')
            continue

        if block.ulid.datetime >= minimum_creation_time:
            LOGGER.info(f'Skip: {str(block.ulid)} is too young.')
            continue

        LOGGER.info(f"Copying: {str(block.ulid)}")
        block_copier.copy_block(block)

    # Write status file to disk
    LOGGER.info("Writing status file to disk.")
    with open(status_file_path, "w") as f:
        json.dump(status, f, indent=4)


def run():
    from os import environ
    prometheus_data_dir = Path(environ.get("PROMETHEUS_DATA_DIR", "/prometheus"))
    export_data_dir = Path(environ.get("TARGET_DATA_DIR", "/export"))
    minimum_age_hours = int(environ.get("MINIMUM_AGE_HOURS", "24"))

    # Make sure export dir exists
    export_data_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        format="%(asctime)s.%(msecs)d|%(levelname)s|%(name)s|%(funcName)s|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
        handlers=[logging.FileHandler(export_data_dir.joinpath(".log"), encoding="utf-8"), logging.StreamHandler()],
    )
    logging.Formatter.converter = time.gmtime

    main(prometheus_data_dir, export_data_dir, minimum_age_hours)


if __name__ == '__main__':
    run()
