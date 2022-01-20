import functools
import logging.config
import multiprocessing
import os
from datetime import date
from datetime import datetime as dt
from pathlib import Path
from typing import List, Mapping, Optional, Tuple, Union

from cdsapi import Client

from miranda.scripting import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)

__all__ = ["request_era5"]


def request_era5(
    variables: Optional[Mapping[str, str]],
    projects: List[str],
    *,
    domain: str = "AMNO",
    output_folder: Optional[Union[str, os.PathLike]] = None,
    year_start: Union[str, int] = 1950,
    year_end: Optional[Union[str, int]] = None,
    processes: int = 10,
) -> None:
    """Request ERA5/ERA5-Land from Copernicus Data Store in NetCDF4 format.

    Parameters
    ----------
    variables: Mapping[str, str]
    projects : List[{"era5", "era5-land"}]
    domain : {"GLOBAL", "AMNO", "CAN", "QC", "MTL"}
    output_folder : str or os.PathLike, optional
    year_start : int
    year_end : int, optional
    processes : int

    Returns
    -------
    None
    """
    # Variables of interest
    variable_reference = dict(
        tp="total_precipitation",
        v10="10m_v_component_of_wind",
        u10="10m_u_component_of_wind",
        d2m="2m_dewpoint_temperature",
        t2m="2m_temperature",
        pev="potential evaporation",
        sde="snow_depth",
        sf="snowfall",
    )

    v_requested = dict()
    if variables:
        for v in variables:
            if v in variable_reference:
                v_requested[v] = variable_reference[v]
    else:
        v_requested = variable_reference

    if year_end is None:
        year_end = date.today().year
    years = range(int(year_start), int(year_end) + 1)

    months = [str(d).zfill(2) for d in range(1, 13)]
    yearmonth = list()
    for y in years:
        for m in months:
            yearmonth.append((y, m))

    project_names = list()
    if "era5" in projects:
        project_names.append("reanalysis-era5-single-levels")
    if "era5-land" in projects:
        project_names.append("reanalysis-era5-land")
    product = project_names[0].split("-")[0]

    if output_folder is None:
        target = Path().cwd().joinpath("downloaded")
    else:
        target = output_folder
    Path(target).mkdir(exist_ok=True)
    os.chdir(target)

    for p in project_names:
        proc = multiprocessing.Pool(processes=processes)
        func = functools.partial(_request_direct_era, v_requested, p, domain, product)

        logging.info([func, dt.now().strftime("%Y-%m-%d %X")])

        proc.map(func, yearmonth)
        proc.close()
        proc.join()


def _request_direct_era(
    variables: Mapping[str, str],
    project: str,
    domain: str,
    product: str,
    yearmonth: Tuple[int, str],
):
    """Launch formatted request."""
    year, month = yearmonth
    days = [str(d).zfill(2) for d in range(32)]
    times = ["{}:00".format(str(t).zfill(2)) for t in range(24)]

    if domain.upper() == "GLOBAL":
        region = [90, -180, -90, 180]
    elif domain.upper() == "AMNO":
        domain = "NAM"
        region = [90, -180, 10, -10]
    elif domain.upper() == "CAN":
        region = [83.5, -141, 41.5, -52.5]
    elif domain.upper() == "QC":
        region = [63, -80, 44.5, -57]
    elif domain.upper() == "MTL":
        region = [45.75, -74.05, 45.3, -73.4]
    else:
        raise ValueError()

    c = Client()

    if project in ["reanalysis-era5-single-levels", "reanalysis-era5-land"]:
        timestep = "hourly"
    else:
        raise NotImplementedError(project)

    for var in variables.keys():
        netcdf_name = (
            f"{var}_{timestep}_ecmwf_{'-'.join(project.split('-')[1:])}"
            f"_{product}_{domain.upper()}_{year}{month}.nc"
        )

        if Path(netcdf_name).exists():
            logging.info("Dataset %s already exists. Continuing..." % netcdf_name)
            continue

        request_kwargs = dict(
            variable=variables[var],
            year=year,
            month=month,
            day=days,
            time=times,
            area=region,
            format="netcdf",
        )

        if project == "reanalysis-era5-single-levels":
            request_kwargs.update(dict(product_type=product))

        c.retrieve(
            project,
            request_kwargs,
            netcdf_name,
        )
