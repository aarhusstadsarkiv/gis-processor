import sqlite3
from os import PathLike
from pathlib import Path
from typing import Any, Union

from gis_processor.utils import MAIN_EXTENSIONS


class GisAVDB:
    """Class for handling call to the given av.db for the handin."""

    def __init__(
        self,
        database: Union[str, bytes, PathLike[str], PathLike[bytes]],
    ) -> None:
        self.connection: sqlite3.Connection = sqlite3.connect(database=database)
        self.cursor: sqlite3.Cursor = self.connection.cursor()
        self.path: Path = Path(database)

    def get_main_gis_files(self) -> list:
        """Returns the main GIS files from a Notes hand-in.

        Returns:
            list: A list of the main files.
        """
        main_files: list = []

        for extention in MAIN_EXTENSIONS:
            result = self.files.select(
                where=f"filename LIKE '%{extention}'",
            ).fetchall()
            main_files.extend(result)

        return main_files

    def get_files_by_template_id(self, template_id) -> list[Any]:  # noqa: ANN001
        """Get files by their template ID.

        Args:
            template_id (__type__): The id of the template to look for

        Returns:
            _type_: The rows where the template ID is the same
        """
        result = self.cursor.execute(
            f"SELECT * FROM fil WHERE notes_template_id = {template_id}",
        )
        rows: list[Any] = result.fetchall()
        return rows
