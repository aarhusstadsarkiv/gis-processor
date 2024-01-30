from datetime import datetime
from pathlib import Path
from typing import Union
from uuid import UUID

from acacore.database.files_db import FileDB
from acacore.models.file import File
from acacore.models.file_data import ActionData, ConvertAction


class GisFilesDB(FileDB):
    """Class for interacting with the `files.db` database, inherits form acacore `FileDB`."""

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
        self.execute(
            """
        UPDATE Files
        SET relative_path = ?
        WHERE uuid == ?;
        """,
            [str(new_rel_path), str(file.uuid)],
        )
        # 3: Make a history entry
        self.add_history(
            uuid=file.uuid,
            operation="gis_processor:move",
            data={
                "update_path_to": new_rel_path,
                "old_path_was": old_rel_path,
                "put_in_template_at_old_path": "Yes",
            },
            reason="Moving all files related to the same gis project together",
            time=datetime.now(),  # noqa: DTZ005
        )
        # 4: We commit the changes to the database
        self.commit()

    def add_template(self, full_path: Path, root_path: Path):
        file = File.from_file(full_path, root_path)
        file.puid = "x-fmt/111"
        file.signature = "Plain Text File"
        file.action = "convert"
        file.action_data = ActionData(convert=[
        ConvertAction(converter="copy", converter_type="master", outputs=["txt"]),
        ConvertAction(converter="text", converter_type="statutory", outputs=["tif"]),
        ],
        )
        self.files.insert(entry=file)
        self.commit()
