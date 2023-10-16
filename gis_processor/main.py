# from logging import root
# from re import template
from pathlib import Path
import json
import shutil
import sys

from gis_processor.utils import EXTENSION_MAPPING
from gis_processor.files_db_gis import GisDB

# Indexes for a row in the fil table of an av.db file.
FILE_ID = 0
NOTES_TEMPLATE_ID = 1
NOTES_TEMPLATE_NAME = 2
FILENAME = 3
DOC_COLLECTION_ID = 4




def find_aux_files(main_file, db_conn: GisDB):
    """Finds the aux. files related to the main file supplied

    Args:
        file (_type_): The file to find aux. files for
        db_conn (GisDB): Connection to the av.db

    Returns:
        _type_: List of aux files
    """
    aux_files = []
    files_by_template_id = db_conn.get_files_by_template_id(main_file[1])

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
                )
            )
    return aux_files


def _place_template(folder_path, moved_to_folder):
    template_file_path = folder_path / "template.txt"
    with open(template_file_path, "w") as file_handle:
        template_content = (
            "This file was part of a gis project.\n It was moved to: {}".format(
                moved_to_folder
            )
        )
        file_handle.write(template_content)


def move_files(aux_files_map, root_dir):
    log_file_path = root_dir / "_metadata" / "gis_processor_log_file.txt"
    log_file = open(log_file_path, "w", encoding="utf-8")

    for master_file_folder in aux_files_map:
        # The folder consists of docCollectionID;docID.
        # so we split it on ";" to get the elements.
        folder_info = master_file_folder.split(";")

        # relative_destination is the last part of the destination (i.e. destination relative to root_dir).
        relative_destination = "docCollection{}/{}".format(
            folder_info[0], folder_info[1]
        )
        destination = root_dir / relative_destination

        # Each aux_file is a triplet (docCollectionID, docID, filename)
        for aux_file in aux_files_map[master_file_folder]:
            absolute_file_path: Path = (
                root_dir / (f"docCollection{aux_file[1]}") / aux_file[0] / aux_file[2]
            )
            if absolute_file_path.exists():
                shutil.move(absolute_file_path, destination)
                _place_template(absolute_file_path.parent, relative_destination)
                log_file.write(
                    "Moved file {} to folder {}\n".format(
                        str(absolute_file_path), str(destination)
                    )
                )

            else:
                log_file.write(
                    f"File already moved: docCollection{aux_file[1]}/{aux_file[0]}/{aux_file[2]}\n"
                )

    log_file.close()


def generate_gis_info(av_db_file_path: str) -> dict:
    """Generates GIS info for the files and dumps it as a .json file and returns it as a dict. 

    Args:
        av_db_file_path (str): The path to the av.db

    Returns:
        dict: GIS info
    """
    db_conn = GisDB(av_db_file_path)
    main_files = db_conn.get_main_gis_files()
    print(f"Found {len(main_files)} main files.")

    aux_files_map = {}

    for file in main_files:
        aux_files = find_aux_files(file)
        # The keys of the aux_files_map are of the form "docCollectionID;fileID"
        key = f"{file[DOC_COLLECTION_ID]};{file[FILE_ID]}"
        aux_files_map[key] = aux_files

    output_file = Path(av_db_file_path).parent / "gis_info.json"
    with open(output_file, "w", encoding="utf-8") as file_handle:
        json.dump(aux_files_map, file_handle, indent=4, ensure_ascii=False)

    return aux_files_map


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


def run_generate_gis_info(av_db_file_path):
    if Path(av_db_file_path).exists():
        aux_files_map = generate_gis_info(av_db_file_path)
        print("Generated gis_info.json file.")
        return aux_files_map
    else:
        print(f"The specified database file does not exist: {av_db_file_path}")
        return None


def main():
    command = None

    if len(sys.argv) > 1:
        command = sys.argv[1]

    if command == "move":
        json_file = input("Enter full path to json file: ")

        aux_files_map = None
        with open(json_file, "r", encoding="utf-8") as f:
            aux_files_map = json.load(f)

        root_dir = input(
            "Enter full path to root folder (OriginalFiles or OriginalDocuments): "
        )
        root_dir_path = Path(root_dir)

        move_files(aux_files_map, root_dir_path)
        print("Finished moving the gis files.")

    elif command == "g-json":
        av_db_file_path = input("Enter full path to av.db file: ")
        run_generate_gis_info(av_db_file_path)

    elif command == "--version":
        print_version()

    elif command == "--help":
        print_help()

    elif command is None:
        av_db_file_path = input("Enter full path to av.db file: ")
        print("Parsing av_db file for gis projects...")
        aux_files_map = run_generate_gis_info(av_db_file_path)

        if aux_files_map is None:
            print("Could not generate gis info.")
        else:
            root_dir = input("Enter full path to root folder (OriginalFiles): ")
            root_dir_path = Path(root_dir)

            move_files(aux_files_map, root_dir_path)
            print("Finished moving the gis files.")

    else:
        print("Invalid arguments. Run python gis_processor.py --help for help.")


if __name__ == "__main__":
    main()
