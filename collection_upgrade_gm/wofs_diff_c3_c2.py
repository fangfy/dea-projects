
# apply wofs classifer to Geomedian and compare classification stats for collection 2 and 3

import glob, os
import xarray as xr
import numpy as np
import datacube

from wofs_classifier import wofs_classify

prod_dc = datacube.Datacube()

outcsv = 'wofs_diff_stats.csv'

c3_paths = glob.glob('/g/data/u46/users/ea6141/stats-tests/gm_c3/LS_GM/*/*')

done_tiles = []
if os.path.exists(outcsv):
    backup = '%s.backup'%outcsv
    os.system('mv %s %s'%(outcsv, backup))
    lines = open(backup).readlines()
    lines =[l for l in lines if len(l.split(','))==6]
    out = open(outcsv, 'w')
    for l in lines:
       print(l.strip(), file=out)
       done_tiles.append(l.split(',')[0])
else:
    out = open(outcsv, 'w')
    print('tile_id, overlap, water_c3, water_c2, waterfrac_c3, waterfrac_c2', file=out)


def get_data(c3_path):
    c3s = []
    c2s = []
    for iband, band in enumerate(['blue', 'green', 'red', 'nir', 'swir1', 'swir2']):
        try:
            c3_file = glob.glob('%s/2015/*_%s.tif'%(c3_path,band))[0]
        except:
            c3_file = glob.glob('%s/2015/*_%s_%s.tif'%(c3_path,band[:-1], band[-1]))[0]
        c3 = xr.open_rasterio(c3_file).to_dataset(dim='band').rename({1:band})
        c2 = prod_dc.load(product='ls8_nbart_geomedian_annual', time=('2015-01-01','2015-02-01'),
                          x=(c3.x.min().values-30,c3.x.max().values+30),
                          y=(c3.y.min().values-30,c3.y.max().values+30),
                          crs="EPSG:3577",
                          measurements=[band],
                 ).interp_like(c3, method='linear')
        c3s.append(c3)
        c2s.append(c2)
    c3_multi = xr.merge(c3s)
    c2_multi = xr.merge(c2s)
    c3_multi, c2_multi = c3_multi.squeeze(), c2_multi.squeeze()
    mask = (c3_multi>0).to_array().all(axis=0).values & (c2_multi>0).to_array().all(axis=0).values
    return c3_multi, c2_multi, mask

nc3 = len(c3_paths)
last = 0
for ic3, c3_path in enumerate(c3_paths):
    tile_x, tile_y = c3_path.split('/')[-2], c3_path.split('/')[-1]
    tile_id = '%s_%s'%(tile_x, tile_y)
    if tile_id in done_tiles: continue
      
    try:
        c3_multi, c2_multi, mask = get_data(c3_path)
    except: 
        continue
    
    water_c3 = wofs_classify(c3_multi, clean_mask = mask, x_coord='x', y_coord='y', mosaic=True)
    n_c3, frac_c3 = water_c3.wofs.where(mask).sum().values, water_c3.wofs.where(mask).mean().values
    water_c2 = wofs_classify(c2_multi, clean_mask = mask, x_coord='x', y_coord='y', mosaic=True)
    n_c2, frac_c2 = water_c2.wofs.where(mask).sum().values, water_c2.wofs.where(mask).mean().values

    print('%s,%.4f,%d,%d,%.6f,%.6f'%(tile_id, mask.mean(), n_c3, n_c2, frac_c3, frac_c2), file=out)

    finish = np.floor(ic3*10./nc3)
    if finish>last: print("%d0 percent finished."%finish)
    last=finish


out.close()

