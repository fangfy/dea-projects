
import matplotlib as mpl
mpl.use('Agg')
from matplotlib import pyplot as plt

import glob, os
import xarray as xr
import numpy as np
import datacube

prod_dc = datacube.Datacube()

plot = True

figdir = 'diff_figs'
if not os.path.exists(figdir): os.mkdir(figdir)

outcsv = 'diff_stats.csv'
#reloutcsv = 'reldiff_stats.csv'

c3_paths = glob.glob('/g/data/u46/users/ea6141/stats-tests/gm_c3/LS_GM/*/*')

done_tiles = []
if os.path.exists(outcsv):
    backup = '%s.backup'%outcsv
    os.system('mv %s %s'%(outcsv, backup))
    lines = open(backup).readlines()
    lines =[l for l in lines if len(l.split(','))==38]
    out = open(outcsv, 'w')
    for l in lines:
       print(l.strip(), file=out)
       done_tiles.append(l.split(',')[0])
else:
    out = open(outcsv, 'w')
    print('tile_id,overlap', end='', file=out)
    for iband, band in enumerate(['blue', 'green', 'red', 'nir', 'swir1', 'swir2']):
        for stat in ['c2med','diffmed','mean','std','p10','p90']:
            print(',%s_%s'%(band,stat),end='', file=out)
    print(file=out)

#relout = open(reloutcsv, 'w')
#print('tile_id,overlap', end='', file=relout)
#for iband, band in enumerate(['blue', 'green', 'red', 'nir', 'swir1', 'swir2']):
#    for stat in ['mean','std','p10','p90']:
#        print(',%s_%s'%(band,stat),end='', file=relout)
#print(file=relout)

nc3 = len(c3_paths)
last = 0
for ic3, c3_path in enumerate(c3_paths):
    tile_x, tile_y = c3_path.split('/')[-2], c3_path.split('/')[-1]
    tile_id = '%s_%s'%(tile_x, tile_y)
    if tile_id in done_tiles: continue
    print(tile_id)

    for iband, band in enumerate(['blue', 'green', 'red', 'nir', 'swir1', 'swir2']):
        try:
            c3_file = glob.glob('%s/2015/*_%s.tif'%(c3_path,band))[0]
        except:
            c3_file = glob.glob('%s/2015/*_%s_%s.tif'%(c3_path,band[:-1], band[-1]))[0]
        try:
            c3 = xr.open_rasterio(c3_file).to_dataset(dim='band').rename({1:band})
            c2 = prod_dc.load(product='ls8_nbart_geomedian_annual', time=('2015-01-01','2015-02-01'),
                              x=(c3.x.min().values-30,c3.x.max().values+30),
                              y=(c3.y.min().values-30,c3.y.max().values+30),
                              crs="EPSG:3577",
                              measurements=[band],
                              ).interp_like(c3, method='linear')
        # percentage difference
            diff = (c3-c2).astype(np.float32).where(c3>-999).where(c2>-999)
            reldiff = ((c3-c2)/c2).where(c3>-999).where(c2>-999).where(c2>1000)
        except:
            print("Problem reading data", c3_file)
            continue

        if iband ==0:
            overlap = (~diff[band].isnull()).mean().values
            print('%s,%.2f'%(tile_id, overlap), end='', file=out)
            #print('%s,%.2f'%(tile_id, overlap), end='', file=relout)

        # plot
        if plot:
            f = plt.figure()
            diff[band].plot.hist(bins=np.arange(-200,200,5), density=True)
            plt.title('(C3-C2)')
            plt.savefig('%s/diff_%s_%s.png'%(figdir,tile_id,band))
            plt.close()
            #f = plt.figure()
            #reldiff[band].plot.hist(bins=np.arange(-0.1,0.1,0.001), density=True)
            #plt.title('(C3-C2)/C2')
            #plt.savefig('%s/reldiff_%s_%s.png'%(figdir,tile_id,band))
            #plt.close()
        # stats
        c2med, diffmed = c2[band].where(c2[band]>-999).median().values, diff[band].median().values
        mean, std, ps = diff[band].mean().values,diff[band].std().values,np.nanpercentile(diff[band],[10,90])
        print(',%.2f,%.2f,%.2f,%.2f,%.2f,%.2f'%(c2med, diffmed, mean, std, ps[0],ps[1]), end='', file=out)
        #mean, std, ps = reldiff[band].mean().values,reldiff[band].std().values,np.nanpercentile(reldiff[band],[10,90])
        #print(',%.2f,%.2f,%.2f,%.2f'%(mean, std, ps[0],ps[1]), end='', file=relout)
    print(file=out)
    #print(file=relout)
    
    finish = np.floor(ic3*10./nc3)
    if finish>last: print("%d0 percent finished."%finish)
    last=finish


out.close()
#relout.close()
