

import subprocess, os, glob
import xarray as xr
#from dask.diagnostics import ProgressBar

inputdir = '/g/data/u46/users/fxy120/CLOUD/LS8_CLOUD_COUNT'
#outputdir = '/g/data/u46/users/fxy120/CLOUD/LS8_CLOUD_PERCENT'

bands = ['cloud_acca_count','cloud_fmask_count','shadow_acca_count','shadow_fmask_count','total_observation_count']
#build vrt
for idx, band in enumerate(bands):
    vrtname = band+'.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/LS8_CLOUD_COUNT_*.nc -sd %d'%(vrtname, inputdir, idx+1)
    subprocess.call(cmd, shell=True)

bands = ['cloud_acca_only_count', 'cloud_fmask_only_count', 'shadow_acca_only_count', 'shadow_fmask_only_count']
#build vrt
for idx, band in enumerate(bands):
    vrtname = band+'.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/LS8_CLOUD_CROSSCOUNT_*.nc -sd %d'%(vrtname, inputdir, idx+1)
    subprocess.call(cmd, shell=True)

# convert to percent
# use xarray
count_files = glob.glob('%s/LS8_CLOUD_COUNT_*.nc'%inputdir)
for cf in count_files:
    percent_file = cf.replace('_COUNT_','_PERCENT_')
    if os.path.exists(percent_file): continue
    ds = xr.open_dataset(cf)
    valid = (ds.total_observation_count >=0) & (ds.cloud_acca_count>=0) & (ds.cloud_fmask_count>=0)
    ds_percent = (ds.cloud_acca_count*100./ds.total_observation_count).round().where(valid, 255).astype('uint8').to_dataset(dim='cloud_acca_percent',name='cloud_percent')
    ds_percent['cloud_fmask_percent'] = (ds.cloud_fmask_count*100./ds.total_observation_count).round().where(valid, 255).astype('uint8')
    ds_percent['cloud_acca_minus_fmask_percent'] = (ds_percent.cloud_acca_percent-ds_percent.cloud_fmask_percent+100).where(valid, 255).astype('uint8')
    ds_percent.attrs=ds.attrs.copy()
    ds_percent.to_netcdf(percent_file)

count_files = glob.glob('%s/LS8_CLOUD_CROSSCOUNT_*.nc'%inputdir)
for cf in count_files:
    total_file = cf.replace('_CROSSCOUNT_','_COUNT_')
    percent_file = cf.replace('_CROSSCOUNT_','_CROSSPERCENT_')
    if os.path.exists(percent_file): continue
    ds = xr.open_dataset(cf)
    ds_total = xr.open_dataset(total_file)
    valid = (ds_total.total_observation_count >=0) & (ds.cloud_acca_only_count>=0) & (ds.cloud_fmask_only_count>=0)
    ds_percent = (ds.cloud_acca_only_count*100./ds_total.total_observation_count).round().where(valid, 255).astype('uint8').to_dataset(dim='cloud_acca_only_percent',name='cloud_percent')
    ds_percent['cloud_fmask_only_percent'] = (ds.cloud_fmask_only_count*100./ds_total.total_observation_count).round().where(valid, 255).astype('uint8')
    ds_percent['shadow_acca_only_percent'] = (ds.shadow_acca_only_count*100./ds_total.total_observation_count).round().where(valid, 255).astype('uint8')
    ds_percent['shadow_fmask_only_percent'] = (ds.shadow_fmask_only_count*100./ds_total.total_observation_count).round().where(valid, 255).astype('uint8')
    ds_percent['cloud_acca_only_percent_fmask'] = (ds.cloud_acca_only_count*100./ds_total.cloud_fmask_count).round().where(valid, 255).astype('uint8')
    ds_percent.attrs=ds.attrs.copy()
    ds_percent.to_netcdf(percent_file)


# mosaic and overview
bands = ['cloud_acca_percent','cloud_fmask_percent','cloud_acca_minus_fmask_percent']
nodata = ['255']*len(bands)
#build vrt                                                                            
for idx, band in enumerate(bands):
    vrtname = band+'.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/LS8_CLOUD_PERCENT_*.nc -sd %d -srcnodata %s -vrtnodata %s'%(vrtname, inputdir, idx+1, nodata[idx], nodata[idx])
    subprocess.call(cmd, shell=True)

for idx, band in enumerate(bands):
    tifname = band+'.tif'
    vrtname = band+'.vrt'
    print(tifname)
    if os.path.exists(tifname): continue
    cmd = 'gdal_translate %s %s -co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -ot Byte -a_srs EPSG:3577 -a_nodata %s'%(vrtname, tifname, nodata[idx])
    subprocess.call(cmd, shell=True)
    cmd = 'gdaladdo -r NEAREST -ro --config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config ZLEVEL 9 %s 4 16 64'%tifname
    subprocess.call(cmd, shell=True)


bands = ['cloud_acca_only_percent','cloud_fmask_only_percent','shadow_acca_only_percent', 'shadow_fmask_only_percent', 'cloud_acca_only_percent_fmask']
nodata = ['255']*len(bands)
#build vrt
for idx, band in enumerate(bands):
    vrtname = band+'.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/LS8_CLOUD_CROSSPERCENT_*.nc -sd %d -srcnodata %s -vrtnodata %s'%(vrtname, inputdir, idx+1, nodata[idx], nodata[idx])
    subprocess.call(cmd, shell=True)

for idx, band in enumerate(bands):
    tifname = band+'.tif'
    vrtname = band+'.vrt'
    print(tifname)
    if os.path.exists(tifname): continue
    cmd = 'gdal_translate %s %s -co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -ot Byte -a_srs EPSG:3577 -a_nodata %s'%(vrtname, tifname, nodata[idx])
    subprocess.call(cmd, shell=True)
    cmd= 'gdaladdo -r NEAREST -ro --config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config ZLEVEL 9 %s 4 16 64'%tifname
    subprocess.call(cmd, shell=True)


#make a smoothed cloud mask to identify problem areas
import numpy as np
from scipy.ndimage import median_filter
from functools import partial
from dask.array import coarsen
from dask import delayed

# 25 m pixel
# downsample to 5 km
# median over 100 km

pixel =25
sampling = 5000
filtersize = 100000
# for downsample

block_size = int(sampling/pixel)

if not os.path.exists('cloud_fmask_percent_downsample.nc'):
    orig_data_array = xr.open_rasterio('cloud_fmask_percent.tif', chunks={'x':4000, 'y':4000})
    data = orig_data_array.chunk().data.squeeze().astype(np.float32)
    data[data==255]=np.nan
    coords = {'band': orig_data_array.coords['band'].values, 
              'y': (orig_data_array.y[::block_size].values+orig_data_array.y[block_size-1::block_size].values)/2,
              'x': (orig_data_array.x[::block_size].values+orig_data_array.x[block_size-1::block_size].values)/2}
    new_data = coarsen(np.median, data, {0: block_size, 1:block_size})
    new_array = xr.DataArray(new_data[np.newaxis,:,:], dims=('band','y','x'), coords=coords).to_dataset(name='cloud_percent')
    result = new_array.to_netcdf('cloud_fmask_percent_downsample.nc',compute=False)
    result.compute()

if not os.path.exists('cloud_fmask_percent_smooth.nc'):
    # for median filter
    rad = int(filtersize/2/sampling)
    y,x = np.ogrid[-1*rad: rad+1, -1*rad: rad+1]
    footprint = x**2.+y**2. <= rad**2.
    smooth = partial(median_filter, footprint=footprint)
    
    orig_data_array = xr.open_rasterio('cloud_fmask_percent_downsample.nc', chunks={'x':4000, 'y':4000})
    data = orig_data_array.chunk().data.squeeze().astype(np.float32)
    data[data==255]=np.nan
    new_data = data.map_overlap(smooth, depth=rad)
    new_array = xr.DataArray(new_data[np.newaxis,:,:], orig_data_array.coords).to_dataset(name='cloud_percent')
    result= new_array.to_netcdf('cloud_fmask_percent_smooth.nc', compute=False)
    result.compute()
