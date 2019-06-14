
# find s1 data overlap with ls8
# save data with ls8 tags

import datacube
from digitalearthau.utils import wofs_fuser
from datacube.storage import masking

import numpy as np
from scipy import ndimage
from skimage.morphology import binary_erosion, disk
import pandas as pd

from s1_water_utils import s1_orbit, denoise

   
def load_clear_wofs(query, dry_months = [10,11,12,1,2], clear_frac = 0.995, valid_frac = 0.75, exclude_ls7 = True):
    prod_dc = datacube.Datacube()

    wofs_dss = prod_dc.find_datasets(product='wofs_albers', **query)
    if dry_months:
        wofs_dss = [ds for ds in wofs_dss 
                     if np.datetime64(ds.metadata_doc['extent']['center_dt']).astype(object).month in dry_months]

    wofs = prod_dc.load(product='wofs_albers', group_by='solar_day', datasets=wofs_dss, fuse_func=wofs_fuser, **query)

    if exclude_ls7:
        invalid = masking.make_mask(wofs.water, noncontiguous=True)
        not_ls7 = invalid.groupby('time').mean().values < .1
        wofs = wofs.isel(time=not_ls7)
        
    valid = np.isin(wofs.water, [0,128])
    valid_check = valid.mean(axis=(1,2)) > valid_frac
    wofs = wofs.isel(time=valid_check)
    
    cloudy = masking.make_mask(wofs.water, cloud=True)
    clear_check  = cloudy.groupby('time').mean().values < 1-clear_frac
    wofs = wofs.isel(time=clear_check)
    
    return wofs


def s1_fuser(dest, src):
    """
    Check for both nan and 0 values
    """
    empty_dest = np.isnan(dest) | (dest==0)
    empty_src = np.isnan(src) | (src==0)
    both = ~empty_dest & ~empty_src
    dest[empty_dest] = src[empty_dest]
    #if len(dest[both])>0:
    #    diff = dest[both] -src[both]
    #    print("mean and max diff (dest-src)", diff.mean(), diff.max() )
    dest[both] = src[both]

    
def load_s1(query, product = 's1_gamma0_scene_v2', wofs_times=None, tol=5):
    dc = datacube.Datacube(config='radar.conf')
    radar_dss = dc.find_datasets(product=product, group_by='solar_day', **query)

    if not wofs_times is None:
        radar_dss = [ds for ds in radar_dss 
                     if np.abs(np.datetime64(
                         ds.metadata_doc['extent']['center_dt'])-wofs_times
                              ).astype('timedelta64[D]').min().astype(int) <= tol+1]


    data = dc.load(product=product, group_by='solar_day', datasets = radar_dss, fuse_func=s1_fuser, **query)
    invalid_fracs = np.isnan(data.to_array()[0]).groupby('time').mean().values
    zero_fracs = (data.to_array()[0]==0).groupby('time').mean().values
    valid_check = (invalid_fracs + zero_fracs) < 0.01   
    data = data.isel(time=valid_check)
    return data.where(data!=0)

def single_orb(radar_dss):
    per_orb ={}
    for ds in radar_dss:
        orb = s1_orbit(str(ds.local_path).split('/')[-1])
        if len(valid_orbs)>0: 
            if not orb in valid_orbs: continue
        if orb in per_orb:
            per_orb[orb].append(ds)
        else:
            per_orb[orb] = [ds]
 
    ndss = 0
    for key in per_orb.keys():
        if len(per_orb[key]) > ndss:
            best_key = key
            ndss = len(per_orb[key])
        
    print(best_key)
    data = dc.load(product='s1_gamma0_scene_v2', group_by='solar_day', datasets = per_orb[best_key], **query)     
    data = data.where(data!=0)

def match_wofs_s1(wofs, s1_data1, s1_data2=None, verbose = False, merge_s1 = True):
    matched_times = pd.merge_asof(wofs.time.to_dataframe(), s1_data1.time.to_dataframe(), 
                            left_index=True, right_index=True, suffixes=('_ls','_s1'),
                            tolerance=pd.Timedelta('5d'), direction='nearest').dropna(how='any')
    if verbose: print(matched_times)
    wofs = wofs.sel(time = matched_times['time_ls'].values)
    s1_data1 = s1_data1.sel(time = matched_times['time_s1'].values)
    
    if not s1_data2 is None:
        matched_times2=pd.merge_asof(s1_data1.time.to_dataframe(), s1_data2.time.to_dataframe(), 
                            left_index=True, right_index=True, suffixes=('_1','_2'),
                            tolerance=pd.Timedelta('1h'), direction='nearest').dropna(how='any')
        if verbose: print(matched_times2)
        wofs = wofs.sel(time = matched_times.set_index('time_s1').loc[matched_times2['time_1']]['time_ls'].values)
        s1_data1 = s1_data1.sel(time = matched_times2['time_1'].values)
        s1_data2 = s1_data2.sel(time = matched_times2['time_2'].values)
        if merge_s1:
            s1_data2['time'] = s1_data1['time']
            s1_data1 = s1_data1.merge(s1_data2)
 
    if (not s1_data2 is None) and (not merge_s1):   
        return wofs, s1_data1, s1_data2
    else:
        return wofs, s1_data1
    
    
def clean_s1(s1_data, **args):
    return denoise(s1_data, **args)
    
    
def clean_wofs(wofs):
    clear_water = wofs.water ==128
    clear = np.isin(wofs.water, [0,128])
    wofs_cent = clear_water.groupby('time').apply(binary_erosion, selem = disk(3))
    wofs['water'] = clear_water.where(clear_water == wofs_cent, 2)
    wofs['water'] = wofs.water.where(clear, 2)
    return wofs




