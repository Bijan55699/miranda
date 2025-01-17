import logging.config
import os
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import GeneratorType
from typing import Dict, Iterable, List, Optional, Sequence, Union

from . import scripting

logging.config.dictConfig(scripting.LOGGING_CONFIG)

__all__ = [
    "chunk_iterables",
    "creation_date",
    "filefolder_iterator",
    "find_filepaths",
    "list_paths_with_elements",
    "read_privileges",
    "single_item_list",
    "working_directory",
    "yesno_prompt",
]

# For datetime validation
ISO_8601 = (
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])"
    r"T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
)


def filefolder_iterator(
    input_files: Union[str, os.PathLike, List[Union[str, os.PathLike]], GeneratorType],
    pattern: str,
) -> Union[List[os.PathLike], GeneratorType]:
    """

    Parameters
    ----------
    input_files: str or Path or List[Union[str, Path]] or GeneratorType
    pattern: str

    Returns
    -------
    list or generator
    """
    if isinstance(input_files, (Path, str)):
        input_files = Path(input_files)
        if input_files.is_dir():
            if pattern.endswith("zarr"):
                input_files = sorted(list(input_files.glob(pattern)))
            else:
                input_files = input_files.rglob(pattern)
    elif isinstance(input_files, list):
        input_files = sorted(Path(p) for p in input_files)
    elif isinstance(input_files, GeneratorType):
        pass
    else:
        raise NotImplementedError(f"input_files: {type(input_files)}")
    return input_files


def chunk_iterables(iterable: Sequence, chunk_size: int) -> List:
    """Generates lists of `chunk_size` elements from `iterable`.

    Notes
    -----
    Adapted from eidord (2012) https://stackoverflow.com/a/12797249/7322852 (https://creativecommons.org/licenses/by-sa/4.0/)
    """
    iterable = iter(iterable)
    while True:
        chunk = list()
        try:
            for _ in range(chunk_size):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk
            break


def creation_date(path_to_file: Union[Path, str]) -> Union[float, date]:
    """
    Try to get the date that a file was created, falling back to when it was last modified if that isn't possible.
    See https://stackoverflow.com/a/39501288/1709587 for explanation.

    Parameters
    ----------
    path_to_file: Union[Path, str]

    Returns
    -------
    Union[float, date]
    """
    if os.name == "nt":
        return Path(path_to_file).stat().st_ctime

    stat = Path(path_to_file).stat()
    try:
        return date.fromtimestamp(stat.st_ctime)
    except AttributeError:
        # We're probably on Linux. No easy way to get creation dates here,
        # so we'll settle for when its content was last modified.
        return date.fromtimestamp(stat.st_mtime)


def read_privileges(location: Union[Path, str], strict: bool = False) -> bool:
    """
    Determine whether a user has read privileges to a specific file

    Parameters
    ----------
    location: Union[Path, str]
    strict: bool

    Returns
    -------
    bool
      Whether the current user shell has read privileges
    """
    if (2, 7) < sys.version_info < (3, 6):
        location = str(location)

    msg = ""
    try:
        if Path(location).exists():
            if os.access(location, os.R_OK):
                msg = f"{location} is read OK!"
                logging.info(msg)
                return True
            msg = f"Ensure read privileges for `{location}`."
        else:
            msg = f"`{location}` is an invalid path."
        raise OSError()

    except OSError:
        logging.exception(msg)
        if strict:
            raise
        return False


@contextmanager
def working_directory(directory: Union[str, Path]) -> None:
    """
    This function momentarily changes the working directory within the context and reverts to the file working directory
    when the code block it is acting upon exits

    Parameters
    ----------
    directory: Union[str, Path]

    Returns
    -------
    None

    """
    owd = os.getcwd()

    if (2, 7) < sys.version_info < (3, 6):
        directory = str(directory)

    try:
        os.chdir(directory)
        yield directory
    finally:
        os.chdir(owd)


def find_filepaths(
    source: Union[Path, str, GeneratorType, List[Union[Path, str]]],
    recursive: bool = True,
    file_suffixes: Optional[Union[str, List[str]]] = None,
    **_,
) -> List[Path]:
    """

    Parameters
    ----------
    source : Union[Path, str, GeneratorType, List[Union[Path, str]]]
    recursive : bool
    file_suffixes: List[str]

    Returns
    -------
    List[Path]
    """

    if file_suffixes is None:
        file_suffixes = ["*", ".*"]
    elif isinstance(file_suffixes, str):
        file_suffixes = [file_suffixes]

    found = list()
    if isinstance(source, (Path, str)):
        source = [source]

    for location in source:
        for pattern in file_suffixes:
            if "*" not in pattern:
                pattern = f"*{pattern}*"
            if recursive:
                found.extend([f for f in Path(location).expanduser().rglob(pattern)])
            elif not recursive:
                found.extend([f for f in Path(location).expanduser().glob(pattern)])
            else:
                raise ValueError(f"Recursive: {recursive}")

    if (2, 7) < sys.version_info < (3, 6):
        found = [str(f) for f in found]

    return found


def single_item_list(iterable: Iterable) -> bool:
    """
    See: https://stackoverflow.com/a/16801605/7322852

    Parameters
    ----------
    iterable: Iterable

    Returns
    -------
    bool

    """
    iterator = iter(iterable)
    has_true = any(iterator)  # consume from "i" until first true or it's exhausted
    has_another_true = any(
        iterator
    )  # carry on consuming until another true value / exhausted
    return has_true and not has_another_true  # True if exactly one true found


########################################################################################


def yesno_prompt(query: str) -> bool:
    """Prompt user for a yes/no answer.
    Parameters
    ----------
    query : str
        the yes/no question to ask the user.

    Returns
    -------
    out : bool
        True (yes) or False (otherwise).
    """

    user_input = input(f"{query} (y/n) ")
    if user_input.lower() == "y":
        return True
    if user_input.lower() == "n":
        return False
    raise ValueError(f"{user_input} not in (y, n)")


def list_paths_with_elements(
    base_paths: Union[str, List[str]], elements: List[str]
) -> List[Dict]:
    """List a given path structure.

    Parameters
    ----------
    base_paths : List[str]
      list of paths from which to start the search.
    elements : List[str]
      ordered list of the expected elements.

    Returns
    -------
    List[Dict]
      The keys are 'path' and each of the members of the given elements, the path is the absolute path.

    Notes
    -----
    Suppose you have the following structure: /base_path/{color}/{shape}
    The resulting list would look like::

         [{'path':/base_path/red/square, 'color':'red', 'shape':'square'},
         {'path':/base_path/red/circle, 'color':'red', 'shape':'circle'},
         {'path':/base_path/blue/triangle, 'color':'blue', 'shape':'triangle'},
         ...]

    Obviously, 'path' should not be in the input list of elements.
    """

    # Make sure the base_paths input is a list of absolute path
    paths = list()
    if not hasattr(base_paths, "__iter__"):
        paths.append(base_paths)
    paths = map(os.path.abspath, base_paths)
    # If elements list is empty, return empty list (end of recursion).
    if not elements:
        return list()

    paths_elements = list()
    for base_path in paths:
        try:
            path_content = [f for f in Path(base_path).iterdir()]
        except NotADirectoryError:
            continue
        path_content.sort()
        next_base_paths = []
        for path_item in path_content:
            next_base_paths.append(base_path.joinpath(path_item))
        next_pe = list_paths_with_elements(next_base_paths, elements[1:])
        if next_pe:
            for i, one_pe in enumerate(next_pe):
                relative_path = next_pe[i]["path"].replace(base_path, "", 1)
                new_element = relative_path.split("/")[1]
                next_pe[i][elements[0]] = new_element
            paths_elements.extend(next_pe)
        elif len(elements) == 1:
            for my_path, my_item in zip(next_base_paths, path_content):
                paths_elements.append({"path": my_path, elements[0]: my_item})
    return paths_elements
