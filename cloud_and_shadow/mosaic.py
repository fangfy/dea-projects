

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

