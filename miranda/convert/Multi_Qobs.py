# read multiple streamflow stations and convert the resulting outpt as a nc file.

import pandas as pd
from glob import glob
import re,os
import xarray as xr
from typing import Optional, Tuple, Union
from shapely.geometry import Point
import geopandas as gpd

# %% input: chnage the path to the directory in which the text files reside.

path = '/home/mohammad/Dossier_travail/Raven/test_Q_deh'


# %% This is exrtacted from the .json file of deh_cf_attrs.json (miranda)
# cf_table = {
#   "Header": {
#     "Conventions": "CF-1.7 CMIP-6.2",
#     "data_specs_version": "00.00.01",
#     "institution": "DEH",
#     "product": "station-obs",
#     "realm": "land",
#     "table_date": "2021-10-26",
#     "table_id": "DEH"
#   },
#   "variable_entry": {
#     "flag": {
#       "comment": "See DEH technical information for details.",
#       "long_name": "data flag"
#     },
#     "qobs": {
#       "long_name": "River discharge",
#       "units": "m3 s-1"
#     }
#   }
# }

meta_patterns = {
    "Station: ": "name",
    "Bassin versant: ": "bv",
    "Coordonnées: (NAD83) ": "coords",
}


data_header_pattern = "Station Date Débit (m³/s) Remarque\n"

# %% calculation block

# def extract_daily(path)  -> Tuple[dict, pd.DataFrame]:
#     """Extract data and metadata from DEH (MELCC) stream flow file."""

#     with open(path, encoding="latin1") as fh:
#         txt = fh.read()
#         txt = re.sub(" +", " ", txt)
#         meta, data = txt.split(data_header_pattern)

#     m = dict()
#     for key in meta_patterns:
#         # Various possible separators to take into account
#         m[meta_patterns[key]] = (
#             meta.split(key)[1].split(" \n")[0].split("\n")[0].split(" Régime")[0]
#         )

#     d = pd.read_csv(
#         path,
#         delimiter=r"\s+",
#         skiprows=len(meta.splitlines()),
#         encoding="latin1",
#         converters={0: lambda x: str(x)},  # noqa
#         index_col=1,
#         parse_dates=True,
#         infer_datetime_format=True,
#     )
#     if len(d["Station"].unique()) == 1:
#         m["station"] = d["Station"].unique()[0]
#         d = d.drop("Station", axis=1)
#     else:
#         raise ValueError("Multiple stations detected in the same file.")
#     d = d.rename(columns={"Remarque": "Nan", "(m³/s)": "Remarque"})
#     d.index.names = ["time"]
#     d = d.drop("Nan", axis=1)

#     return m, d


# def to_cf(meta: dict, data: pd.DataFrame, cf_table) -> xr.Dataset:
#     """Return CF-compliant metadata."""
#     ds = xr.Dataset()

#     ds["q"] = xr.DataArray(d["Débit"], attrs=cf_table['variable_entry']['q'])
#     ds["flag"] = xr.DataArray(data["Remarque"], attrs=cf_table["flag"])

#     ds["name"] = xr.DataArray(meta["name"])
#     ds["station_id"] = xr.DataArray(meta["station"])

#     ds["area"] = xr.DataArray(
#         u.convert(float(meta["bv"].split(" ")[0]), meta["bv"].split(" ")[1], "km²"),
#         attrs={"long_name": "drainage area", "units": "km2"},
#     )

#     def parse_dms(coord):
#         deg, minutes, seconds, _ = re.split("[°'\"]", coord)
#         if float(deg) > 0:
#             return round(
#                 float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60), 6
#             )
#         return round(float(deg) - (float(minutes) / 60 + float(seconds) / (60 * 60)), 6)

#     coords = m["coords"].split(" // ")
#     ds["lat"] = xr.DataArray(
#         parse_dms(coords[0]),
#         attrs={
#             "standard_name": "latitude",
#             "long_name": "latitude",
#             "units": "decimal_degrees",
#         },
#     )
#     ds["lon"] = xr.DataArray(
#         parse_dms(coords[1]),
#         attrs={
#             "standard_name": "longitude",
#             "long_name": "longitude",
#             "units": "decimal_degrees",
#         },
#     )

#     ds.attrs[
#         "institution"
#     ] = "Ministère de l'Environnement et de la Lutte contre les changements climatiques"
#     ds.attrs[
#         "source"
#     ] = "Hydrometric data <https://www.cehq.gouv.qc.ca/hydrometrie/historique_donnees/index.asp>"
#     ds.attrs["redistribution"] = "Redistribution policy unknown. For internal use only."

#     return ds
# %%
os.chdir(path)
files = glob ("*.txt")
dataset = pd.DataFrame(columns = ["station_id"])
latitude = pd.DataFrame(columns={'station_id','lat'},index = None)
longitude = pd.DataFrame(columns={'station_id','lon'},index = None)

for i in range(len(files)): 
    fname = os.path.join(path,files[i])
    print("writing station:",files[i])
    with open(fname, encoding="latin1") as fh:
        txt = fh.read()
        txt = re.sub(" +", " ", txt)
        meta, data = txt.split(data_header_pattern)

    m = dict()
    for key in meta_patterns:
    # Various possible separators to take into account
        m[meta_patterns[key]] = (
            meta.split(key)[1].split(" \n")[0].split("\n")[0].split(" Régime")[0]
            )
    d = pd.read_csv(
    fname,
    delimiter=r"\s+",
    skiprows=len(meta.splitlines()),
    encoding="latin1",
    converters={0: lambda x: str(x)},  # noqa
    index_col=1,
    parse_dates=True,
    infer_datetime_format=True,
)
    d = d.drop(['Remarque','(m³/s)'], axis=1)
    d=d.dropna()
    if len(d["Station"].unique()) == 1:
        m["station"] = d["Station"].unique()[0]
        d = d.drop("Station", axis=1)
    else:
        raise ValueError("Multiple stations detected in the same file.")
    d = d.rename(columns={"Débit":'qobs'})
    d.index.names = ["time"]
#    d = d.drop("Nan", axis=1)
    
    # finding latitude (in decimal degree)
    coords = m["coords"].split(" // ")  
    deg, minutes, seconds, _ = re.split("[°'\"]", coords[0])

    if float(deg) > 0:
        lat =  round(
            float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60), 6
        )
    else:
        lat =  round(float(deg) - (float(minutes) / 60 + float(seconds) / (60 * 60)), 6)

    # finding longitude (in decimal degree)
    coords = m["coords"].split(" // ")  
    deg, minutes, seconds, _ = re.split("[°'\"]", coords[1])

    if float(deg) > 0:
        lon =  round(
            float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60), 6
        )
    else:
        lon =  round(float(deg) - (float(minutes) / 60 + float(seconds) / (60 * 60)), 6)
    
    d['lon'] = lon
    d['lat'] = lat
    d['station_id'] = m['station']
    
    lt = pd.DataFrame(d,columns={'station_id','lat'},index = None)
    lo = pd.DataFrame(d,columns={'station_id','lon'},index = None)
    d = d.drop("lat", axis=1)
    d = d.drop("lon", axis=1)

    
    dataset = pd.concat([dataset,d],join = 'outer')
    latitude = pd.concat([latitude,lt],join = 'outer')
    longitude = pd.concat([longitude,lo],join = 'outer')
    
    latitude = latitude.drop_duplicates(subset = ["station_id"])
    longitude = longitude.drop_duplicates(subset = ["station_id"])
    
    
    del d,m,coords,lat,lon,deg,minutes,seconds,fname,meta,data,lt,lo

dataset = dataset.rename_axis('time').reset_index()


dataset = dataset.set_index(['station_id', 'time'])
dataset = dataset[~dataset.index.duplicated(keep='first')]

ds = dataset.to_xarray()


ds2 = latitude.set_index(['station_id','lat']).to_xarray()
ds3 = longitude.set_index(['station_id','lon']).to_xarray()

ds_merged = xr.merge([ds, ds2,ds3])

ds.to_netcdf("SLSO_qobs2.nc",'w')




# %% to use with shapefile script finding the subbasin outlets with gauges
dataset2 = pd.merge(latitude, longitude, on='station_id')
gdf_points = gpd.GeoDataFrame(
    dataset2, geometry=gpd.points_from_xy(dataset2.lon, dataset2.lat))

gdf_points = gdf_points.set_crs(epsg = 4326)

subbasin = gpd.read_file('/home/mohammad/Dossier_travail/Hydrotel/DEH/HRUs_Quebec_meridional/20P/subbasin.shp')

subbasin = subbasin.to_crs(4326) # EPSG=4326 (WGS84)


dataset3 = gpd.sjoin(gdf_points,subbasin,how = 'left',op='within')

dataset4 = dataset3[['geometry','station_id','SubId']]

dataset4.to_file('/home/mohammad/Dossier_travail/Hydrotel/DEH/HRUs_Quebec_meridional/20P/SLSO_gauges.shp')
