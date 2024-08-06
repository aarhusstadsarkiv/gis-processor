from abc import ABC
from abc import abstractmethod
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor
from sqlite3 import Row
from typing import Any
from typing import Generator
from typing import Type


class Processor(ABC):
    _main_extensions: list[str] = [
        ".mif",
        ".shp",
        ".tab",
    ]
    _aux_extensions: dict[str, list[str]] = {
        ".mif": [
            ".mid",
        ],
        ".shp": [
            ".aih",
            ".ain",
            ".atx",
            ".cpg",
            ".dbf",
            ".fbn",
            ".fbx",
            ".ixs",
            ".mxs",
            ".prj",
            ".sbn",
            ".sbx",
            ".shx",
            ".xml",
        ],
        ".tab": [
            ".dat",
            ".id",
            ".ind",
            ".map",
        ],
    }

    def __init__(self, conn: Connection):
        self.conn: Connection = conn

    @property
    def main_extensions(self):
        return sorted(set(self._main_extensions), key=self._main_extensions.index)

    @classmethod
    @abstractmethod
    def is_valid(cls, connection: Connection) -> bool: ...

    @abstractmethod
    def file_to_path(self, file: dict[str, Any]) -> str | PathLike: ...

    @abstractmethod
    def find_main_files(self) -> Generator[dict[str, Any], None, None]: ...

    @abstractmethod
    def find_auxiliary_files(self, main_file: dict[str, Any]) -> Generator[dict[str, Any], None, None]: ...


# noinspection SqlNoDataSourceInspection,SqlResolve
class CiriusNotesProcessor(Processor):
    _tables: list[str] = ["dokument", "dokument_dokument", "dokument_fil", "fil", "sag", "sag_dokument"]

    @classmethod
    def is_valid(cls, connection: Connection) -> bool:
        tables: list[str] = [
            n.lower() for [n] in connection.execute("select name from sqlite_master where type = 'table'")
        ]
        if all(t in tables for t in cls._tables):
            return True

    def file_to_path(self, file: dict[str, Any]) -> str | PathLike:
        doc_collection: int = int(file["doc_collection_id"])
        file_id: int = int(file["fil_id"])
        return Path(f"docCollection{doc_collection}", str(file_id), file["filename"])

    def find_main_files(self) -> Generator[dict[str, Any], None, None]:
        where: str = " or ".join(f"filename like '%' || ?" for _ in self.main_extensions)
        cursor: Cursor = self.conn.cursor()
        cursor.execute(f"select * from fil where {where}", self.main_extensions)
        cursor.row_factory = Row
        yield from (dict(f) for f in cursor)
        cursor.close()

    def find_auxiliary_files(self, main_file: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
        cursor: Cursor = self.conn.cursor()
        cursor.execute(
            "select * from fil where notes_template_id = ?",
            [main_file["notes_template_id"]],
        )
        cursor.row_factory = Row
        yield from (
            dict(f)
            for f in cursor.fetchall()
            if Path(f["filename"]).stem == Path(main_file["filename"]).stem
            and Path(f["filename"]).suffix.lower() in self._aux_extensions[Path(main_file["filename"]).suffix.lower()]
        )
        cursor.close()


def find_processor(conn: Connection) -> Type[Processor] | None:
    cls: Type[Processor]
    return next((cls for cls in (CiriusNotesProcessor,) if cls.is_valid(conn)), None)
