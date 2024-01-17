# from logging import root
# from re import template
import json
import shutil
import sys
from os import PathLike
from pathlib import Path
from typing import Union

from gis_processor.av_db_gis import GisAVDB
from gis_processor.files_db_gis import GisFilesDB
from gis_processor.utils import EXTENSION_MAPPING

# Indexes for a row in the fil table of an av.db file.
FILE_ID = 0
NOTES_TEMPLATE_ID = 1
NOTES_TEMPLATE_NAME = 2
FILENAME = 3
DOC_COLLECTION_ID = 4


class GisProcessor:
    """Class for handling methods related to GIS-processing."""

    def __init__(self, av_db_path: Union[Path, None] = None, file_db_path: Union[Path, None] = None) -> None:
        self.av_db: Union[GisAVDB, None] = GisAVDB(av_db_path) if av_db_path else None
        self.file_db: Union[GisFilesDB, None] = GisFilesDB(file_db_path) if file_db_path else None

    def find_aux_files(self, main_file: list):
        """Finds the aux. files related to the main file supplied.

        Args:
            main_file (_type_): The file to find aux. files for

        Returns:
            _type_: List of aux files
        """
        aux_files = []
        files_by_template_id = self.av_db.get_files_by_template_id(main_file[1])

        for possible_aux_file in files_by_template_id:
            file_as_path = Path(possible_aux_file[FILENAME])
            main_file_path = Path(main_file[FILENAME])
            # If the files have the same stem and possible_aux_file has a suffix in
            # the aux suffix list for the main file format, then we add it to aux_files.
            if (
                file_as_path.stem == main_file_path.stem
                and file_as_path.suffix in EXTENSION_MAPPING[main_file_path.suffix]
            ):
                aux_files.append(
                    (
                        possible_aux_file[FILE_ID],
                        possible_aux_file[DOC_COLLECTION_ID],
                        possible_aux_file[FILENAME],
                    ),
                )
        return aux_files

    def _place_template(self, folder_path: Path, moved_to_folder: PathLike):
        template_file_path = folder_path / "template.txt"
        with open(template_file_path, "w") as file_handle:
            template_content = f"This file was part of a gis project.\n It was moved to: {moved_to_folder}"
            file_handle.write(template_content)

    def move_files(self, aux_files_map: dict[str, list[list[str]]], root_dir: Path):
        """_summary_.

        Args:
            aux_files_map (dict): _description_
            root_dir (Path): _description_
        """
        log_file_path: Path = root_dir / "_metadata" / "gis_processor_log_file.txt"
        log_file = open(log_file_path, "w", encoding="utf-8")  # noqa: SIM115

        for master_file_folder in aux_files_map:
            # The folder consists of docCollectionID;docID.
            # so we split it on ";" to get the elements.
            folder_info: list[str] = master_file_folder.split(";")

            # relative_destination is the last part of the destination (i.e. destination relative to root_dir)
            relative_destination: Path = Path(f"docCollection{folder_info[0]}") / Path(folder_info[1])
            destination: Path = root_dir / relative_destination

            # Each aux_file is a triplet (docCollectionID, docID, filename)
            for aux_file in aux_files_map[master_file_folder]:
                relative_path: Path = Path(
                    (f"docCollection{aux_file[1]}") / Path(aux_file[0]) / Path(aux_file[2]),
                )
                absolute_file_path: Path = root_dir / relative_path
                if absolute_file_path.exists():
                    shutil.move(absolute_file_path, destination)
                    self._place_template(absolute_file_path.parent, relative_destination)
                    log_file.write(
                        f"Moved file {absolute_file_path!s} to folder {destination!s}\n",
                    )
                    new_rel_path_for_aux_file: Path = relative_destination / Path(aux_file[2])
                    # Then we update the newly moved file in the files.db
                    self.file_db.update_rel_path(
                        new_rel_path=new_rel_path_for_aux_file,
                        old_rel_path=relative_path,
                    )

                else:
                    log_file.write(
                        f"File already moved: docCollection{aux_file[1]}/{aux_file[0]}/{aux_file[2]}\n",
                    )

        log_file.close()

    def generate_gis_info(self) -> dict:
        """Generates GIS info for the files and dumps it as a .json file and returns it as a dict.

        Returns:
            dict: Dictionary of GIS info
        """
        main_files: list = self.av_db.get_main_gis_files()
        print(f"Found {len(main_files)} main files.")

        aux_files_map: dict = {}

        for file in main_files:
            aux_files = self.find_aux_files(main_file=file)
            # The keys of the aux_files_map are of the form "docCollectionID;fileID"
            key = f"{file[DOC_COLLECTION_ID]};{file[FILE_ID]}"
            aux_files_map[key] = aux_files

        output_file = Path(self.av_db.path).parent / "gis_info.json"
        with open(output_file, "w", encoding="utf-8") as file_handle:
            json.dump(aux_files_map, file_handle, indent=4, ensure_ascii=False)

        return aux_files_map

    def run_generate_gis_info(self):
        if self.av_db is None:
            print("Error: The path to the av.db was not correct. Please verify.")
        self.generate_gis_info()


def print_help():
    help_message = (
        "Invoke the tool by running python gis_processor.py with the commands\n"
        "   * g-json. Generate the gis_info.json file.\n"
        "   * move. Move files according to the gis_info.json file.\n"
        "Running the script with no commands defaults to g-json followed by move"
    )
    print(help_message)


def print_version() -> None:
    version: str = "Ukendt version"
    with open(Path(__file__).absolute().parent.parent / "pyproject.toml") as i:
        for line in i.readlines():
            if line.startswith("version"):
                version = line[line.index('"') + 1 : -2]
    print(version)


def main():
    command = None

    if len(sys.argv) > 1:
        command = sys.argv[1]

    if command == "move":
        json_file = input("Enter full path to json file: ")

        aux_files_map = None
        with open(json_file, encoding="utf-8") as f:
            aux_files_map = json.load(f)

        root_dir: Path = Path(input("Enter full path to root folder (OriginalFiles or OriginalDocuments): "))
        files_db_path: Path = Path(input("Enter full path to files.db file: "))

        processor = GisProcessor(file_db_path=files_db_path)

        processor.move_files(aux_files_map, root_dir)
        print("Finished moving the gis files.")

    elif command == "g-json":
        av_db_file_path = input("Enter full path to av.db file: ")
        processor = GisProcessor(av_db_path=av_db_file_path)
        processor.run_generate_gis_info()

    elif command == "--version":
        print_version()

    elif command == "--help":
        print_help()

    elif command is None:
        av_db_file_path: str = input("Enter full path to av.db file: ")
        files_db_path: str = input("Enter full path to files.db file: ")
        print("Parsing av_db file for gis projects...")
        processor = GisProcessor(av_db_path=av_db_file_path, file_db_path=files_db_path)
        aux_files_map: dict = processor.generate_gis_info()

        if aux_files_map is None:
            print("Could not generate gis info.")
        else:
            root_dir_path: Path = Path(files_db_path).parent.parent

            processor.move_files(aux_files_map, root_dir_path)
            print("Finished moving the gis files.")

    else:
        print("Invalid arguments. Run python gis_processor.py --help for help.")


if __name__ == "__main__":
    main()
