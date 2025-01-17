import warnings
from os import getenv
from pathlib import Path
from tempfile import tempdir

try:
    from dask.distributed import Client

    warnings.warn(
        "Dask and Distributed are strongly suggested for performing rechunking operations on large datasets."
    )

    has_dask = True
except ImportError:
    Client = None
    has_dask = False

from miranda.convert import rechunk_reanalysis

if __name__ == "__main__":
    step = "hourly"  # "daily
    target_project = "era5-land"  # "era5-single-levels"
    outfmt = "nc"  # "zarr"

    in_files = getenv("in")
    out_files = getenv("out")

    input_path = Path(in_files)
    output_path = Path(out_files)

    parameters = dict(
        project=target_project,
        input_folder=input_path,
        output_folder=output_path,
        time_step=step,
        output_format=outfmt,
    )

    if has_dask:
        with Client(
            n_workers=2,
            threads_per_worker=1,
            dashboard_address=8786,
            memory_limit="4GB",
            local_directory=Path(tempdir),
        ):
            rechunk_reanalysis(**parameters)
    else:
        rechunk_reanalysis(**parameters)
