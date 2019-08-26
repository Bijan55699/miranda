#!/bin/env python3
import logging
import os
import re
import tarfile
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime as dt
from getpass import getpass
from math import pow
from pathlib import Path
from types import GeneratorType
from typing import Iterable
from typing import List
from typing import Mapping

import fabric
from paramiko import AuthenticationException
from paramiko import SFTPClient
from paramiko import SSHClient
from paramiko import SSHException
from scp import SCPClient
from scp import SCPException


Nested_List = List[List[Path]]
PathDict = Mapping[str, List[Path]]

GiB = int(pow(2, 30))


def make_remote_directory(directory, transport: SSHClient or fabric.Connection):
    """
    This calls a function to create a folder structure over SFTP/SSH and waits
     for confirmation before continuing
    """
    logging.info(
        "{}: Creating remote path: {}".format(
            dt.now().strftime("%Y-%m-%d %X"), directory
        )
    )

    ownership = "0775"
    command = "mkdir -p -m {} '{}'".format(ownership, directory)
    if isinstance(transport, fabric.Connection):
        transport.run(command)
    elif isinstance(transport, (SSHClient, SCPClient)):
        transport.exec_command(command, timeout=1)
        for i in range(5):
            if not directory.exists():
                time.sleep(1)
                continue
            break
    return


@contextmanager
def working_directory(directory):
    """
    This function momentarily changes the working directory within the
     context and reverts to the file working directory when the code block
     it is acting upon exits
    """
    owd = os.getcwd()
    try:
        os.chdir(directory)
        yield directory
    finally:
        os.chdir(owd)
    return


def _transfer_single(
    source_file: Path or str,
    destination: Path or str,
    transport: SCPClient or SFTPClient or fabric.Connection,
) -> bool:
    try:
        logging.info(
            "{}: Passing {}".format(dt.now().strftime("%Y-%m-%d %X"), source_file)
        )
        transport.put(str(source_file), str(destination))
        logging.info(
            "{}: Transferred {} to {}".format(
                dt.now().strftime("%Y-%m-%d %X"),
                Path(destination).name,
                Path(destination).parent,
            )
        )
    except SCPException or SSHException or IOError or OSError as e:
        msg = '{}: File "{}" failed to be added: {}.'.format(
            dt.now().strftime("%Y-%m-%d %X"), destination.name, e
        )
        logging.warning(msg)
        return False
    return True


def _transfer_archive(
    source_files: List[Path or str],
    destination: Path or str,
    transport: SCPClient or SFTPClient or fabric.Connection,
    compression: bool = False,
    recursive: bool = True,
) -> bool:
    if compression:
        write = "w:gz"
    elif not compression:
        write = "w"
    else:
        raise ValueError("Compression: {}".format(compression))

    with tempfile.NamedTemporaryFile() as temp:
        archive_file = temp.name
        with tarfile.open(archive_file, write) as tar:
            for name in source_files:
                try:
                    logging.info(
                        "{}: Tarring {}".format(
                            dt.now().strftime("%Y-%m-%d %X"), name.name
                        )
                    )
                    tar.add(name.relative_to(Path.cwd()), recursive=recursive)
                except Exception as e:
                    msg = '{}: File "{}" failed to be tarred: {}'.format(
                        dt.now().strftime("%Y-%m-%d %X"), name.name, e
                    )
                    logging.warning(msg)

            tar.close()

        try:
            logging.info(
                "{}: Beginning scp transfer of {} to {}".format(
                    dt.now().strftime("%Y-%m-%d %X"),
                    Path(destination).name,
                    Path(destination).parent,
                )
            )
            transport.put(str(archive_file), str(destination))

        except SCPException or SSHException or IOError or OSError as e:
            msg = '{}: File "{}" failed to be added: {}.'.format(
                dt.now().strftime("%Y-%m-%d %X"), destination.name, e
            )
            logging.warning(msg)
            return False

        logging.info(
            "{}: Transferred {} to {}".format(
                dt.now().strftime("%Y-%m-%d %X"),
                Path(destination).name,
                Path(destination).parent,
            )
        )
    return True


def file_size(
    file_path_or_bytes: str or Path or int,
    use_binary: bool = True,
    significant_digits: int = 2,
) -> str or None:
    """
    This function will return the size in bytes of a file or a list of files
    """

    conversions = ["B", "k{}B", "M{}B", "G{}B", "T{}B", "P{}B", "E{}B", "Z{}B", "Y{}B"]

    def _size_formatter(i: int, binary: bool = True, precision: int = 2) -> str:
        """
        This function will format byte size into an appropriate nomenclature
        """
        import math

        base = 1024 if binary else 1000
        if i == 0:
            return "0 B"
        multiple = math.trunc(math.log2(i) / math.log2(base))
        value = i / math.pow(base, multiple)
        suffix = conversions[multiple].format("i" if binary else "")
        return "{value:.{precision}f} {suffix}".format(**locals())

    if isinstance(file_path_or_bytes, int):
        return _size_formatter(
            file_path_or_bytes, binary=use_binary, precision=significant_digits
        )
    elif isinstance(file_path_or_bytes, (list, GeneratorType)):
        sizes = [Path(f).stat().st_size for f in file_path_or_bytes]
        total = sum(sizes)
    elif Path(file_path_or_bytes).is_file():
        total = Path(file_path_or_bytes).stat().st_size
    else:
        return

    return _size_formatter(total, binary=use_binary, precision=significant_digits)


def single_item_list(iterable: Iterable) -> bool:
    """
    See: https://stackoverflow.com/a/16801605/7322852
    """
    iterator = iter(iterable)
    has_true = any(iterator)  # consume from "i" until first true or it's exhausted
    has_another_true = any(
        iterator
    )  # carry on consuming until another true value / exhausted
    return has_true and not has_another_true  # True if exactly one true found


def group_by_length(files: list or GeneratorType, size: int = 10) -> Nested_List:
    """
    This function groups files by an arbitrary number of file entries
    """

    grouped_list = list()
    group = list()
    if isinstance(files, GeneratorType):
        files = [f for f in files]
    files.sort()

    logging.info(
        "{}: Creating groups of {} files".format(dt.now().strftime("%Y-%m-%d %X"), size)
    )

    for i, f in enumerate(files):
        group.append(f)
        if (i + 1) % size == 0:
            grouped_list.append(group.copy())
            group.clear()
            continue

    if not group:
        logging.info(
            "{}: The final group is empty. Skipping...".format(
                dt.now().strftime("%Y-%m-%d %X")
            )
        )
    else:
        grouped_list.append(group.copy())
    return grouped_list


def group_by_deciphered_date(files: list or GeneratorType) -> PathDict:
    """
    This function attempts to find a common date and groups files based on year and month
    """
    if isinstance(files, GeneratorType):
        files = [Path(f) for f in files]
    files.sort()

    logging.info(
        "{}: Creating files from deciphered dates.".format(
            dt.now().strftime("%Y-%m-%d %X")
        )
    )

    year_month_day = re.compile(
        r"(?P<year>[0-9]{4})-?(?P<month>[0-9]{2})-?(?P<day>[0-9]{2})?.*\.(?P<suffix>nc)$"
    )

    dates = defaultdict(lambda: list())
    total = 0
    for f in files:
        match = re.search(year_month_day, str(f.name))
        if match.group("day"):
            key = "-".join([match.group("year"), match.group("month")])
            dates[key].append(Path(f))
            total += 1
        elif match.group("month"):
            key = match.group("year")
            dates[key].append(Path(f))
            total += 1
        else:
            continue

    now = dt.now()
    if dates and total == len(files):
        logging.info(
            "{}: All files have been grouped by date.".format(
                now.strftime("%Y-%m-%d %X")
            )
        )
        return dates

    elif dates and total != len(files):
        logging.info(
            "{}: Not all files were successfully grouped by date. Grouping aborted.".format(
                now.strftime("%Y-%m-%d %X")
            )
        )
    else:
        logging.info(
            "{}: No matches for dates found. Grouping aborted.".format(
                now.strftime("%Y-%m-%d %X")
            )
        )
    return dict(data=files)


def group_by_size(files: list or GeneratorType, size: int = 10 * GiB) -> Nested_List:
    """
    This function will group files up until a desired size and save it as a grouping within a list
    """
    grouped_list = list()
    group = list()
    if isinstance(files, GeneratorType):
        files = [f for f in files]
    files.sort()

    logging.info(
        "{}: Creating groups of files based on size not exceeding {}".format(
            dt.now().strftime("%Y-%m-%d %X"), file_size(size)
        )
    )

    total = 0
    for f in files:
        total += Path.stat(f).st_size
        group.append(f)
        if total > size:
            grouped_list.append(group.copy())
            group.clear()
            total = 0
            continue
        elif total < size:
            continue

    if not group:
        logging.info(
            "{}: The final group is empty. Skipping...".format(
                dt.now().strftime("%Y-%m-%d %X")
            )
        )
    else:
        grouped_list.append(group.copy())
    return grouped_list


def group_by_subdirectories(
    files: list or GeneratorType, within: str or Path = None
) -> PathDict:
    """
    This function will group files based on the parent folder that they are located within.
    """
    groupings = defaultdict(list)
    if isinstance(files, GeneratorType):
        files = [f for f in files]
    files.sort()

    if not within:
        within = Path.cwd()

    for f in files:
        group_name = Path(f).relative_to(within).parent
        groupings[group_name].append(f)

    logging.info(
        "{}: File subdirectories found. Proceeding with {}.".format(
            dt.now().strftime("%Y-%m-%d %X"),
            str([str(key) for key in groupings.keys()]),
        )
    )
    return groupings


def archive(
    source: str or Path or List,
    common_source: str or Path,
    target: str or Path,
    server: str = None,
    username: str = None,
    password: str = None,
    project_name: str = None,
    overwrite: bool = False,
    compression: bool = False,
    recursive: bool = False,
    use_grouping: bool = True,
    use_subdirectories: bool = True,
) -> None:
    """
    Given a source, destination, and dependent on file size limit, create tarfile archives and transfer
     files to another server for backup purposes
    """
    project = "{}_{}{}.{}"

    if not project_name:
        project_name = target.name

    if compression:
        suffix = "tar.gz"
    elif not compression:
        suffix = "tar"
    else:
        raise ValueError("Compression: {}".format(compression))

    if recursive:
        pattern = "**/*.nc"
    elif not recursive:
        pattern = "*.nc"
    else:
        raise ValueError("Recursive: {}".format(recursive))

    if isinstance(source, (GeneratorType, List)):
        file_list = [f for f in source]
        source_path = Path().cwd().anchor
    else:
        file_list = [f for f in Path(source).glob(pattern)]
        source_path = Path(source)

    if use_subdirectories:
        file_groups = group_by_subdirectories(file_list, within=common_source)

    else:
        file_groups = defaultdict(lambda: list())
        for f in file_list:
            file_groups["."].append(f)

    try:
        user = username or input("Enter username: ")
        pw = password or getpass("Enter password: ")
        connection = fabric.Connection(
            host=server, user=user, connect_kwargs=dict(password=pw)
        )
    except AuthenticationException as e:
        logging.error("{}: Unable to connect to remote host {}.".format(e, server))
        raise

    try:
        successful_transfers = list()
        with connection as ctx:
            for group_name, members in file_groups.items():
                remote_path = Path(target, group_name)

                if not remote_path.exists():
                    make_remote_directory(remote_path, transport=ctx)

                if use_grouping:
                    dated_groups = group_by_deciphered_date(members)
                else:
                    dated_groups = dict(group_name=members)

                for common_date, files in dated_groups.items():
                    if not use_grouping or single_item_list(files):
                        for archive_file in files:
                            transfer = Path(remote_path, archive_file.name)

                            if transfer.is_file():
                                if not overwrite:
                                    logging.info(
                                        "{}: {} exists. Skipping file.".format(
                                            dt.now().strftime("%Y-%m-%d %X"), transfer
                                        )
                                    )
                                    continue
                                logging.info(
                                    "{}: {} exists. Overwriting.".format(
                                        dt.now().strftime("%Y-%m-%d %X"), transfer
                                    )
                                )

                            if _transfer_single(archive_file, transfer, transport=ctx):
                                successful_transfers.append(archive_file)

                    elif use_grouping or not single_item_list(files):
                        sized_groups = group_by_size(files)

                        for i, sized_group in enumerate(sized_groups):
                            if len(sized_groups) > 1:
                                part = "_{}".format(str(i + 1).zfill(3))
                            else:
                                part = ""

                            archive_file = project.format(
                                project_name, group_name, common_date, part, suffix
                            )
                            transfer = Path(remote_path, archive_file)

                            if transfer.is_file():
                                if not overwrite:
                                    logging.info(
                                        "{}: {} exists. Skipping file.".format(
                                            dt.now().strftime("%Y-%m-%d %X"), transfer
                                        )
                                    )
                                    continue
                                logging.info(
                                    "{}: {} exists. Overwriting.".format(
                                        dt.now().strftime("%Y-%m-%d %X"), transfer
                                    )
                                )

                            with working_directory(source_path):
                                if _transfer_archive(
                                    sized_group,
                                    transfer,
                                    transport=ctx,
                                    compression=compression,
                                    recursive=recursive,
                                ):
                                    successful_transfers.extend(sized_group)
                    else:
                        raise FileNotFoundError("No files found in grouping.")

        logging.info(
            "Transferred {} of {} files totalling {} at {}.".format(
                len(successful_transfers),
                len([f for f in file_list]),
                file_size(successful_transfers),
                dt.now().strftime("%Y-%m-%d %X"),
            )
        )

    except Exception as e:
        msg = "{}: {} Failed to transfer files.".format(
            dt.now().strftime("%Y-%m-%d %X"), e
        )
        logging.error(msg)
        raise RuntimeError(msg) from e

    return


if __name__ == "__main__":
    logging.basicConfig(
        filename="{}_{}.log".format(
            dt.strftime(dt.now(), "%Y%m%d"), Path(__name__).stem
        ),
        level=logging.INFO,
    )