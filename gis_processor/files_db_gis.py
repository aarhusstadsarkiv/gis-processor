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
        db_path = Path(database)
        super().__init__(database=db_path)

    def update_rel_path(
        self,
        new_rel_path: Path,
        uuid: Union[UUID, None] = None,
        old_rel_path: Union[Path, None] = None,
    ):
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
            file: Union[File, None] = self.files.select(where="uuid = ?", parameters=[str(uuid)]).fetchone()
        elif old_rel_path:
            file: Union[File, None] = self.files.select(
                where="relative_path = ?",
                parameters=[str(old_rel_path)],
            ).fetchone()
        else:
            file = None

        if not file:
            raise FileNotFoundError(
                "The file could not be found given either the UUID or relative path given. "
                "Please check the parameters",
            )

        # 2: Update the relative path of the file to the new relative path
        file.relative_path = new_rel_path
        self.files.insert(file, replace=True)
        # 3: Make a history entry
        self.add_history(
            uuid=file.uuid,
            operation="gis_processor:move",
            data=None,
            reason="GIS processing and rearranging",
            time=datetime.now(),  # noqa: DTZ005
        )
