

import subprocess, os, glob
import xarray as xr
#from dask.diagnostics import ProgressBar

inputdir = '/g/data/u46/users/fxy120/CLOUD/S2_CLOUD_COUNT'

bands = ['cloud_fmask_count','shadow_fmask_count','total_observation_count']
#build vrt
for idx, band in enumerate(bands):
    vrtname = band+'_s2.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/S2_CLOUD_COUNT_*.nc -sd %d'%(vrtname, inputdir, idx+1)
    subprocess.call(cmd, shell=True)

# convert to percent
# use xarray
count_files = glob.glob('%s/S2_CLOUD_COUNT_*.nc'%inputdir)
for cf in count_files:
    percent_file = cf.replace('_COUNT_','_PERCENT_')
    if os.path.exists(percent_file): continue
    ds = xr.open_dataset(cf)
    valid = (ds.total_observation_count >=0) & (ds.cloud_fmask_count>=0)
    ds_percent = (ds.cloud_fmask_count*100./ds.total_observation_count).round().where(valid, 255).astype('uint8').to_dataset(dim='cloud_fmask_percent',name='cloud_percent')
    ds_percent.attrs=ds.attrs.copy()
    ds_percent.to_netcdf(percent_file)

# mosaic and overview
bands = ['cloud_fmask_percent']
nodata = ['255']*len(bands)
#build vrt                                                                            
for idx, band in enumerate(bands):
    vrtname = band+'_s2.vrt'
    print(vrtname)
    if os.path.exists(vrtname): continue
    cmd = 'gdalbuildvrt %s %s/S2_CLOUD_PERCENT_*.nc -sd %d -srcnodata %s -vrtnodata %s'%(vrtname, inputdir, idx+1, nodata[idx], nodata[idx])
    subprocess.call(cmd, shell=True)

for idx, band in enumerate(bands):
    tifname = band+'_s2.tif'
    vrtname = band+'_s2.vrt'
    print(tifname)
    if os.path.exists(tifname): continue
    cmd = 'gdal_translate %s %s -tr 25 25 -r nearest -co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -ot Byte -a_srs EPSG:3577 -a_nodata %s'%(vrtname, tifname, nodata[idx])
    subprocess.call(cmd, shell=True)
    cmd = 'gdaladdo -r NEAREST -ro --config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config ZLEVEL 9 %s 4 16 64'%tifname
    subprocess.call(cmd, shell=True)


#difference between LS8 and S2
if not os.path.exists('cloud_fmask_percent_s2_minus_ls8.nc'):
    p1 = xr.open_rasterio('cloud_fmask_percent_s2.tif', chunks={'x':5000, 'y':5000})
    p2 = xr.open_rasterio('cloud_fmask_percent.tif', chunks={'x':5000, 'y':5000})
    nodata = 255
    diff = (p1+100-p2).where(p1!=nodata, nodata).where(p2!=nodata, nodata)
    result=diff.to_netcdf('cloud_fmask_percent_s2_minus_ls8.nc',compute=False)
    result.compute()

