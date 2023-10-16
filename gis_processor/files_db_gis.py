from os import PathLike
from typing import Union
from acacore.database import FileDB
from gis_processor.utils import MAIN_EXTENSIONS


class GisDB(FileDB):
    def __init__(
        self, database: Union[str, bytes, PathLike[str], PathLike[bytes]]
    ) -> None:
        super().__init__(database=database)

    def get_main_gis_files(self) -> list:
        """Returns the main GIS files as a of Notes hand-in

        Returns:
            list: A list of the main files.
        """

        main_files = []

        for extention in MAIN_EXTENSIONS:
            result = self.execute(
                f"SELECT * FROM fil WHERE filename LIKE '%{extention}'"
            ).fetchall()
            main_files.extend(result)

        return main_files

    def get_files_by_template_id(self, template_id):
        """Get files by their template ID

        Args:
            template_id (__type__): The id of the template to look for

        Returns:
            _type_: The rows where the template ID is the same
        """
        result = self.execute(
            f"SELECT * FROM fil WHERE notes_template_id = {template_id}"
        )
        rows = result.fetchall()
        return rows
