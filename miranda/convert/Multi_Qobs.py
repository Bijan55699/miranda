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

meta_patterns = {
    "Station: ": "name",
    "Bassin versant: ": "bv",
    "Coordonnées: (NAD83) ": "coords",
}


data_header_pattern = "Station Date Débit (m³/s) Remarque\n"

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
    #d=d.dropna()
    d = d.fillna(-1.2345)  # fill NaNs with -1.2345
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

ds.to_netcdf("SLSO_qobs.nc",'w')

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
