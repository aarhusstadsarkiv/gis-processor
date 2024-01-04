from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import Union
from uuid import UUID

from acacore.database.files_db import FileDB
from acacore.models.file import File


class GisFilesDB(FileDB):
    """Class for interacting with the `files.db` database, inherits form acacore `FileDB`."""

    def __init__(self, database: Union[str, bytes, PathLike[str], PathLike[bytes]]) -> None:
        super.__init__(database=database)

    def update_rel_path(
        self, new_rel_path: Path, uuid: Union[UUID, None] = None, old_rel_path: Union[Path, None] = None
    ):  # noqa: E501
        """Update the relative path of a file in the db based on either its UUID or its old relative path.

        Args:
            new_rel_path (Path): The new realtive path of the file
            uuid (Union[UUID, None], optional): The uuid of the file. Defaults to None.
            old_rel_path (Union[Path, None], optional): The old relative path of the file. Defaults to None.

        Raises:
            FileNotFoundError: If the file can't be found in the database based on either rel_path or uuid
        """ """"""
        # 1: Locate the file either using the given uuid or relative path
        if uuid:
            file: Union[File, None] = self.files.select(where="uuid = ?", parameters=uuid).fetchone()
        elif old_rel_path:
            file: Union[File, None] = self.files.select(
                where="relative_path = ?", parameters=old_rel_path
            ).fetchone()
        else:
            file = None

        if not file:
            raise FileNotFoundError(
                "The file could not be found given either the UUID or relative path given. Please check the parameters",
            )

        # 2: Update the relative path of the file to the new relative path
        self.execute(
            """
            UPDATE files
            SET relative_path = ?
            WHERE uuid = ?
            """,
            parameters=[new_rel_path, file.uuid],
        )

        # 3: Make a history entry
        self.add_history(
            uuid=file.uuid,
            operation="gis_processor:move",
            data=None,
            reason="GIS processing and rearranging",
            time=datetime.now(),
        )  # noqa: E501, DTZ005
