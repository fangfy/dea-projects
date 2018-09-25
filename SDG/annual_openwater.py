
import glob, os, sys
import subprocess
import xarray as xr
from dask.diagnostics import ProgressBar

if len(sys.argv)>1:
    threshold = int(sys.argv[1])
else:
    threshold = 75 #percent, more than 9 months

csize = 4000

# make a mask that matches wofs extent
maskshape = '/g/data/u46/users/fxy120/sensor_data_maps/Australia/Australian_Coast_and_Islands_Albers.shp'
masklayer = 'Australian_Coast_and_Islands_Albers'
maskfile = 'mask.tif'
if not os.path.exists(maskfile):
    cmd = 'gdal_rasterize -burn 1 -a_srs EPSG:3577 -a_nodata 0 -tr 25 -25 -te -2000000. -5000000. 2500000. -1000000. -co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -ot Byte -l %s %s %s'%(masklayer, maskshape, maskfile)
    print(cmd)
    subprocess.call(cmd, shell=True)
    
inputpath = '/g/data/u46/users/bt2744/work/data/wofs/output/prod/'
annual_vrts = glob.glob(inputpath + 'wofs_????_annual_summary.vrt')
annual_vrts.sort()
annual_vrts = annual_vrts[::-1]

print("Year, # of pixels with more than %d percent detections, extent (km^2)"%threshold)

for annual_vrt in annual_vrts[6:19]:
    
    year = os.path.basename(annual_vrt).split('_')[1]
    data=xr.open_rasterio(annual_vrt, chunks={'x':csize, 'y':csize})
    mask=xr.open_rasterio('mask.tif', chunks={'x':csize, 'y':csize})

    thresh_float = threshold/100.

    with ProgressBar(dt=60):
        masked=(mask*(data>thresh_float)).sum().values
    print(year, masked, masked*25.*25./1e6)


                         
        
