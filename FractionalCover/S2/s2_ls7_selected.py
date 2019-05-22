

import sys
# Import external functions from dea-notebooks
sys.path.append('/g/data/u46/users/fxy120/dea-notebooks/10_Scripts/')
import DEAPlotting, DEADataHandling

import numpy as np
import xarray as xr
import pandas as pd
from collections import OrderedDict

from fc_utils import convert_s2_to_ls7, convert_s2_to_ls8, convert_ls8_to_ls7, ls8_ls7_coefficients
from fc_utils import compute_fc, downsample_fc, compare_fc_combined
from fc_utils import s2_ls7_coefficients, s2_ls8_coefficients

import matplotlib.pyplot as plt
from matplotlib import gridspec

import datacube

 
def compare_s2_ls7_at_location(easting, northing):
    s2dc=datacube.Datacube(config='/home/120/fxy120/s2_ard.conf')
    lsdc=datacube.Datacube()
    
    hsize=10000
    x = (easting+hsize,easting-hsize)   # 12,500 m is half of the required window size
    y = (northing+hsize,northing-hsize)

    ls_bands=['blue','green','red','nir','swir1','swir2']
    s2_bands_translate=OrderedDict({'nbart_blue':'blue',
                                    'nbart_green':'green',
                                    'nbart_red':'red',
                                    'nbart_nir_1':'nir',
                                    'nbart_swir_2':'swir1',
                                    'nbart_swir_3':'swir2'})
    s2_bands=list(s2_bands_translate.keys()) + ['fmask']

    query = {
        'time': ('2017-01-01','2017-07-01'), # for west australia
        'x': x,
        'y': y,
        'crs': 'EPSG:3577',
        'output_crs': 'EPSG:3577',
        }
    s2query = query.copy()
    lsquery = query.copy()
    lsquery['resolution']=(-25,25)
    s2query['resolution']=(-10,10)

    s2data = DEADataHandling.load_clearsentinel(s2dc, s2query, sensors=['s2a'], bands_of_interest=s2_bands,
                                                product='ard', masked_prop=0.90, mask_values=[0, 2, 3], apply_mask=True, 
                                                pixel_quality_band='fmask')
    s2data = s2data.rename(s2_bands_translate)
    s2data.attrs['name']='Sentinel-2'
    fcdata = DEADataHandling.load_clearlandsat(lsdc, lsquery, sensors=['ls7'], bands_of_interest=None,
                                               product='fc', masked_prop=0.60, mask_dict=None, apply_mask=True, ls7_slc_off=True)
    fcdata.attrs['name']='Landsat 7 FC'

    #filter to match observation times
    matched_times=pd.merge_asof(s2data.time.to_dataframe(), fcdata.time.to_dataframe(), left_index=True, right_index=True, 
                                tolerance=pd.Timedelta('1d'), direction='nearest').dropna(how='any')

    if len(matched_times)==0: return

    s2data=s2data.sel(time=matched_times['time_x'].values)
    fcdata=fcdata.sel(time=matched_times['time_y'].values)

    plt.figure(figsize=(12,5))
    gs = gridspec.GridSpec(1,2) # set up a 2 x 2 grid of 4 images for better presentation
    
    scene=-1
    ax1=plt.subplot(gs[0])
    RGB_bands=['red','green','blue']
    s2data[RGB_bands].isel(time=scene).to_array().plot.imshow(robust=True, ax=ax1)
    ax1.set_title('Sentinel-2')
    
    ax2=plt.subplot(gs[1])
    RGB_bands=['BS','PV','NPV']
    fcdata.where(fcdata.UE<20)[RGB_bands].isel(time=scene).to_array().plot.imshow(robust=True, ax=ax2)
    ax2.set_title('FC')
    
    plt.tight_layout()
    plt.savefig('S2_and_FC_{0}_{1}'.format(easting, northing))
                
    return

    fc_s2 = compute_fc(s2data)
    # resample
    fc_ls_50m = downsample_fc(fcdata,factor=2)
    fc_s2_50m = downsample_fc(fc_s2,factor=5)

    title='S2 uncorrected vs Landsat 7 FC at xy {0} {1}'.format(easting, northing)
    compare_fc_combined(fc_s2_50m, fc_ls_50m, title=title, savefig=True)

    # S2 converted to equivalent Landsat 8/7
    s2_corrected_ls7 = convert_s2_to_ls7(s2data)
    s2_corrected_ls7.attrs['name']='Sentinel-2 corrected to LS7'
    fc_s2_corrected_ls7 = compute_fc(s2_corrected_ls7, regression_coefficients=None)
    fc_s2_corrected_ls7_50m = downsample_fc(fc_s2_corrected_ls7, 5)
    title='S2 corrected vs Landsat 7 FC at xy {0} {1}'.format(easting, northing)
    compare_fc_combined(fc_s2_corrected_ls7_50m, fc_ls_50m, title=title, savefig=True)


for easting, northing in [(1540000, -3920000), (-70000, -2750000), (-1080000, -3530000)]:
    compare_s2_ls7_at_location(easting, northing)

