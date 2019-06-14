
import numpy as np
from astropy.stats import histogram
from scipy.signal import argrelextrema


def find_optimal_minimum(hist, hist_x, min_valid=0.001, max_valid=0.03, min_contrast=1.2, min_perc= 0.01, 
                         verbose=False):
    minima= hist_x[argrelextrema(hist, np.less, order=1)]
    maxima= hist_x[argrelextrema(hist, np.greater, order=1)]
    # no minima or maxima found
    if len(minima)==0 or len(maxima)==0: 
        if verbose: print('no minima or maxima')
        return None
    # minima not in approprite range
    if minima.max() < min_valid or minima.min() > max_valid: 
        if verbose: print('minima outside valid range')
        return None
    minima=minima[minima>=min_valid]
    minima=minima[minima<=max_valid]
    # find minimum with largest contrast
    contrast=0
    best_m=None
    hist_maxima=hist[argrelextrema(hist, np.greater, order= 1)]
    for m in minima:
        # has to have a maximum on lower side
        lower_peaks=hist_maxima[maxima<m]
        if len(lower_peaks)==0: continue
        # has to have more than min_perc below threshold
        # note the histograms don't extend full range of values, so 0.1 means much less than 10% overall
        # this can potentially be constrained from minimum extent from wofs
        if hist[:np.where(hist_x==m)[0][0]+1].sum()/hist.sum()<min_perc: continue
        low=hist[np.where(hist_x==m)[0]][0]    
        this_contrast=lower_peaks.max()/low
        if this_contrast>contrast:
            contrast=this_contrast
            best_m=m
    if best_m is None or contrast<min_contrast: 
        if verbose: print('Minimum percent or minimum contrast is not met.')
        return None
    return hist_x[np.where(hist_x==best_m)[0]+1][0]  # right boundary


def threshold_vv(clean, band = 'hist_vv_classified'):
    
    water = np.zeros_like(clean.vv)

    for i in range(len(clean.time)):
        arr = clean.vv.isel(time=i).values.ravel()
        arr = arr[~np.isnan(arr)]
        # shorten the array to speed up
        arr = arr[arr < np.percentile(arr, 90)]
        hist, hist_x = histogram(np.random.choice(arr, int(len(arr)/3)), bins='knuth')
        minima= find_optimal_minimum(hist, hist_x, min_valid=0.001, max_valid=0.03, min_contrast=1.2, min_perc= 0.005)
        if minima:
            water[i,:,:]=clean.vv.isel(time=i) <= minima
        else:
            water[i,:,:] = np.nan
    clean[band] = ('time','y','x'), water
    
    
def threshold_vh(clean, band = 'hist_vh_classified', min_perc =0.01):
    
    water = np.zeros_like(clean.vh)

    for i in range(len(clean.time)):
        arr = clean.vh.isel(time=i).values.ravel()
        arr = arr[~np.isnan(arr)]
        # shorten the array to speed up
        arr = arr[arr < np.percentile(arr, 90)]
        hist, hist_x = histogram(np.random.choice(arr, int(len(arr)/3)), bins='knuth')
        minima= find_optimal_minimum(hist, hist_x, min_valid=0.001, max_valid=0.007, min_contrast=1.2, min_perc=min_perc)
        if minima:
            water[i,:,:]=clean.vh.isel(time=i) <= minima
        else:
            water[i,:,:] = np.nan
    clean[band] = ('time','y','x'), water

