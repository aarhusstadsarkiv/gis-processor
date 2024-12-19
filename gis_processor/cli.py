from logging import ERROR
from logging import INFO
from os import PathLike
from pathlib import Path
from shutil import copy
from sqlite3 import connect
from uuid import UUID
from uuid import uuid4

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import OriginalFile
from acacore.models.reference_files import IgnoreAction
from acacore.utils.click import check_database_version
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import command
from click import Context
from click import option
from click import pass_context
from click import Path as ClickPath
from click import version_option

from .__version__ import __version__
from .processor import find_processor
from .processor import Processor


def file_not_found_error(
    ctx: Context,
    file_type: str,
    path: str | PathLike,
    uuid: UUID | None,
) -> Event:
    return Event.from_command(ctx, f"file.{file_type}:error", (uuid, "original"), None, f"{path} not found")


def relative_path(root: Path, original_documents: Path, path: str | PathLike) -> Path:
    return original_documents.joinpath(path).relative_to(root)


@command("gis-processor")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument("avid", nargs=1, type=ClickPath(exists=True, dir_okay=False, readable=True, resolve_path=True))
@option("--dry-run", is_flag=True, default=False, help="Show changes without committing them.")
@version_option(__version__)
@pass_context
def app(ctx: Context, root: str | PathLike, avid: str | PathLike, dry_run: bool):
    """Gis-processor copies all auxiliary gis files to the directory of their corresponding mandatory main file.

    \b
    ROOT    Path to the root of the relevant documents directory, e.g. 'OriginalDocuments'
    AVID    Path to the '...av.db', e.g. 'AVID.AARS.61.1/_metadata/AVID.AARS.61.av.db'

    The 'av.db'-dababase is needed as it contains information on the original relations between the related gis files.

    The 'action' of the original auxiliary file is set to 'template' as its original presence must be documented.

    The 'action' of the copied auxiliary file is set to 'ignore' as the gis converter handles the files internally.
    """

    root, avid = Path(root), Path(avid)
    database_path: Path = root / "_metadata" / "avid.db"
    original_documents: Path = root / "OriginalDocuments"

    if not database_path.is_file():
        raise BadParameter(f"No _metadata/avid.db present in {root!r}.", ctx, ctx_params(ctx)["root"])
    if not original_documents.is_dir():
        raise BadParameter(f"No OriginalDocuments present in {root!r}.", ctx, ctx_params(ctx)["root"])

    check_database_version(ctx, ctx_params(ctx)["root"], database_path)

    with connect(avid) as avid_conn:
        if not (processor_cls := find_processor(avid_conn)):
            raise ValueError(f"{avid!r} is not recognised")
        processor: Processor = processor_cls(avid_conn)

        with FilesDB(database_path) as db:
            log_file, log_stdout, _ = start_program(ctx, db, __version__, None, not dry_run, True, dry_run)

            with ExceptionManager() as exception:
                for main_file_orig in processor.find_main_files():
                    main_file_path: Path = relative_path(
                        root,
                        original_documents,
                        processor.file_to_path(main_file_orig),
                    )
                    if not root.joinpath(main_file_path).is_file():
                        Event.from_command(ctx, f"file.main:error", reason="File does not exists").log(
                            ERROR,
                            log_stdout,
                            path=str(main_file_path),
                        )
                        continue

                    main_file: OriginalFile | None = db.original_files[{"relative_path": str(main_file_path)}]

                    if not main_file:
                        Event.from_command(ctx, f"file.main:error", reason="Not in database").log(
                            ERROR,
                            log_stdout,
                            path=str(main_file_path),
                        )
                        continue

                    aux_files: list[tuple[OriginalFile, OriginalFile]] = []

                    for aux_file_orig in processor.find_auxiliary_files(main_file_orig):
                        aux_file_path: Path = relative_path(
                            root,
                            original_documents,
                            processor.file_to_path(aux_file_orig),
                        )

                        if not root.joinpath(aux_file_path).is_file():
                            Event.from_command(
                                ctx,
                                f"file.aux:error",
                                reason="File does not exists",
                            ).log(ERROR, log_stdout)
                            aux_files = []
                            break

                        aux_file: OriginalFile | None = db.original_files[{"relative_path": str(aux_file_path)}]

                        if not aux_file:
                            Event.from_command(
                                ctx,
                                f"file.aux:error",
                                reason="Not in database",
                            ).log(
                                ERROR,
                                log_stdout,
                                path=str(aux_file_path),
                            )
                            aux_files = []
                            break

                        new_path: Path = main_file.relative_path.with_name(aux_file.name)
                        aux_file.lock = True
                        aux_file.action = "ignore"
                        aux_file.action_data.ignore = IgnoreAction(template="text", reason=f"Moved to {new_path}")

                        aux_file_copy: OriginalFile

                        if aux_file_copy := db.original_files[{"relative_path": str(new_path)}]:
                            if aux_file_copy.checksum != aux_file.checksum:
                                Event.from_command(
                                    ctx,
                                    "file.aux:error",
                                    reason="File already exists with different hash",
                                ).log(ERROR, log_stdout, path=str(new_path))
                                aux_files = []
                                break
                        elif root.joinpath(new_path).is_file():
                            Event.from_command(ctx, "file.aux:error", reason="File already exists").log(
                                ERROR,
                                log_stdout,
                                path=str(new_path),
                            )
                            aux_files = []
                            break
                        else:
                            aux_file_copy = aux_file.model_copy(
                                update={"uuid": uuid4(), "relative_path": new_path},
                                deep=True,
                            )

                        aux_file_copy.action = "ignore"
                        aux_file_copy.action_data.ignore = IgnoreAction(template="temporary-file")
                        aux_files.append((aux_file, aux_file_copy))

                    for aux_file, aux_file_copy in aux_files:
                        event = Event.from_command(
                            ctx,
                            "file.aux:copy",
                            (aux_file_copy.uuid, "original"),
                            [str(aux_file.relative_path), str(aux_file_copy.relative_path)],
                        )

                        event.log(INFO, log_stdout, main=str(main_file.relative_path))

                        if dry_run:
                            continue

                        try:
                            copy(aux_file.get_absolute_path(root), aux_file_copy.get_absolute_path(root))
                            db.original_files.insert(aux_file_copy, on_exists="replace")
                            db.original_files.update(aux_file)
                            db.log.insert(event)
                        except BaseException:
                            aux_file_copy.get_absolute_path(root).unlink(missing_ok=True)
                            raise

            end_program(ctx, db, exception, dry_run, log_file, log_stdout)
