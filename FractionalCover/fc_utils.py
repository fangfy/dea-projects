
from fc.fractional_cover import compute_fractions
from datacube.utils import read_documents
#from datacube.storage.masking import valid_data_mask

from pathlib import Path
from collections import OrderedDict
import xarray as xr
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
from matplotlib import gridspec
from matplotlib.colors import LogNorm
from scipy.stats import pearsonr, spearmanr, kendalltau


app_config_file='/g/data/v10/public/modules/agdc-fc/fc-1.2.0-20-g3aaa2a8/config/ls8_fc_albers.yaml'
app_config_path = Path(app_config_file)
_, config = next(read_documents(app_config_path))
# required by FC: output_measurements and regression_coefficients 
var_def_keys = {'name', 'dtype', 'nodata', 'units', 'aliases', 'spectral_definition', 'flags_definition'}
measurements=[{k: v for k, v in measurement.items() if k in var_def_keys} 
              for measurement in config['measurements']]
output_measurements = OrderedDict((m['name'], m) for m in measurements).values()

#coefficients to convert ls8 to ls7
ls8_ls7_coefficients = config['sensor_regression_coefficients']

#coefficients to convert s2 to ls7
s2_ls7_coefficients = {'blue':[-0.0022, 0.9551],
                       'green':[0.0031, 1.0582],
                       'red':[0.0064, 0.9871],
                       'nir':[0.012, 1.0187],
                       'swir1':[0.0079, 0.9528],
                       'swir2':[-0.0042, 0.9688]}

#coeffients to convert s2 to ls8
s2_ls8_coefficients = {'blue':[-0.0012, 0.963],
                       'green':[0.0013, 1.0473],
                       'red':[0.0027, 0.9895],
                       'nir':[0.0147, 1.0129],
                       'swir1':[0.0025, 0.9626],
                       'swir2':[-0.0011, 0.9392]}

def convert_sr(ds, coef):
    ds_corrected = ds.copy().astype(np.float32)
    # mask no data
    ds_corrected = ds_corrected.where(ds_corrected.to_array()>0, drop=False)
    for band in list(coef.keys()):
        ds_corrected[band] = (coef[band][0] + coef[band][1] * ds[band]/1e4)*1e4
    return ds_corrected.fillna(0)

def convert_ls8_to_ls7(ds):
    return convert_sr(ds, coef=ls8_ls7_coefficients)

def convert_s2_to_ls7(ds):
    return convert_sr(ds, coef=s2_ls7_coefficients)

def convert_s2_to_ls8(ds):
    return convert_sr(ds, coef=s2_ls8_coefficients)

def compute_fc(input_ds, #input_nodata=-999,
               fc_bands=['green','red','nir','swir1','swir2'],
               regression_coefficients=None,
               output_measurements=output_measurements):
    
    band_names = ['PV', 'NPV', 'BS', 'UE']
    output_ds = xr.Dataset(coords={#'time': input_ds.time[:], 
                                'y': input_ds.y[:], 'x': input_ds.x[:],
                                'band': band_names,
                                }, attrs=input_ds.attrs)
    for time_index in range(input_ds.time.count().values):
        input_data=input_ds[fc_bands].isel(time=time_index).to_array().data
        is_valid_array= (input_data >0).all(axis=0)
        # Set nodata to 0                                                       
        input_data[:, ~is_valid_array] = 0

        # compute fractional_cover
        output_data = compute_fractions(input_data, regression_coefficients)
        
        nodata_values = {}
        for measurement in output_measurements:
            src_var = measurement.get('src_var', None) or measurement.get('name')
            i = band_names.index(src_var)
            # Set nodata value into output array
            band_nodata = np.dtype(measurement['dtype']).type(measurement['nodata'])
            compute_error = (output_data[i, :, :] == -1)
            output_data[i, compute_error] = band_nodata
            output_data[i, ~is_valid_array] = band_nodata
            #nodata_values[band_names[i]]=band_nodata

        # re-arrange bands
        output_ds[input_ds.time.values[time_index]]= (('band','y','x'), output_data)
    
    return output_ds.to_array(dim='time').to_dataset(dim='band')[['BS', 'PV', 'NPV', 'UE']]

def downsample_fc(fc_ds,factor):
    fc_ds=fc_ds.where(fc_ds.UE>0).where(fc_ds.UE<=30)
    rollingx = fc_ds.rolling(x=factor, center=False).construct('window_x',stride=factor).mean('window_x')
    rolled = rollingx.rolling(y=factor, center=False).construct('window_y',stride=factor).mean('window_y')
    rolled.attrs = fc_ds.attrs
    return rolled

def compare_fc_ds(fc_s2, fc_ls, plot=True, title=None):
    regr = linear_model.LinearRegression(fit_intercept=False)    

    if plot:
        plt.figure(figsize=(16,6))
        gs = gridspec.GridSpec(1,3) # set up a 2 x 2 grid of 4 images for better presentation

        xedges=yedges=list(np.arange(0,102,2))
        X, Y = np.meshgrid(xedges, yedges)
        cmname='YlGnBu'
        if title: plt.suptitle(title)
    
    rmses = []
    for band_id, band in enumerate(['BS','PV','NPV']):
        arr1 = fc_s2[band].values.ravel()
        arr2 = fc_ls[band].values.ravel()
        arre = fc_ls['UE'].values.ravel()
        good = (arr1 > 0) & (arr2>0) & (arre<20)
        goodfrac= good.sum()*1./arr1.shape[0]
        if goodfrac<0.2: continue
        #print("Fraction of good pixels:",goodfrac)
        arr1 = arr1[good]
        arr2 = arr2[good]
        regr.fit(arr2[:,np.newaxis], arr1[:,np.newaxis])
        # The coefficients
        print('Band:{0}, slope={1}, r2={2}'.format(band, regr.coef_[0][0],
                                                regr.score(arr2[:,np.newaxis], arr1[:,np.newaxis])))
        print('Correlations:', pearsonr(arr1, arr2)[0], spearmanr(arr1, arr2)[0], kendalltau(arr1, arr2)[0])
        rmse = np.sqrt(mean_squared_error(arr1, arr2))
        print('RMSE:',rmse)
        rmses.append(rmse)
        
        if plot:
            ax1 = plt.subplot(gs[band_id])
            hist, xe, ye= np.histogram2d(arr1, arr2, bins=(xedges, yedges))
            plt.pcolor(X,Y,hist, cmap=cmname, norm=LogNorm(1, vmax=hist.max()))
            plt.colorbar()
            if fc_s2.name: ax1.set_xlabel(fc_s2.name)
            if fc_ls.name: ax1.set_ylabel(fc_ls.name)
            ax1.set_title(band)
            ax1.plot([0,100],[0,100])
            ax1.plot(np.arange(0,100,10), regr.predict(np.arange(0,100,10)[:,np.newaxis]), ':')
            
    return rmses

def compare_fc_sets(fc_s2_50m, fc_ls_50m):
    rmses=[]
    ntimes=fc_s2_50m.time.count().values
    for time in range(ntimes):
        plot=True
        print(fc_s2_50m.time[time].values)
        rmse = compare_fc_ds(fc_s2_50m.isel(time=time), fc_ls_50m.isel(time=time), plot=plot, title=fc_s2_50m.time[time].values)
        rmses.append(rmse)
    
    print("average RMSE:", np.array(rmses).mean(axis=0))

def compare_fc_combined(fc_s2_50m, fc_ls_50m, plot=True, title=None, savefig=None):    

    if fc_s2_50m.name: name_s2= fc_s2_50m.name
    if fc_ls_50m.name: name_ls= fc_ls_50m.name

    arr_s2=fc_s2_50m.stack(z=('x','y'))
    arr_s2=arr_s2.where(arr_s2.UE<20, drop=False).to_array().data
    arr_s2=arr_s2.reshape(arr_s2.shape[0], arr_s2.shape[1]*arr_s2.shape[2])
    arr_ls=fc_ls_50m.stack(z=('x','y'))
    arr_ls=arr_ls.where(arr_ls.UE<20, drop=False).to_array().data
    arr_ls=arr_ls.reshape(arr_ls.shape[0], arr_ls.shape[1]*arr_ls.shape[2])
    
    regr = linear_model.LinearRegression(fit_intercept=False)
    
    if plot:
        plt.figure(figsize=(16,6))
        gs = gridspec.GridSpec(1,3) # set up a 2 x 2 grid of 4 images for better presentation                                         
        
        xedges=yedges=list(np.arange(0,102,2))
        X, Y = np.meshgrid(xedges, yedges)
        cmname='YlGnBu'
        if title: plt.suptitle(title)

    rmses = []
    for band_id, band in enumerate(['BS','PV','NPV']):
        arr1 = arr_s2[band_id]
        arr2 = arr_ls[band_id]
        good = (arr1 > 0) & (arr2>0)
        arr1 = arr1[good]
        arr2 = arr2[good]
        regr.fit(arr2[:,np.newaxis], arr1[:,np.newaxis])

        # The coefficients                                                                                                             
        print('Band:{0}, slope={1}, r2={2}'.format(band, regr.coef_[0][0],
                                                regr.score(arr2[:,np.newaxis], arr1[:,np.newaxis])))
        sr = spearmanr(arr1, arr2)[0]
        print('Correlations:', pearsonr(arr1, arr2)[0], sr, kendalltau(arr1, arr2)[0])
        rmse = np.sqrt(mean_squared_error(arr1, arr2))
        print('RMSE:',rmse)
        rmses.append(rmse)

        if plot:
            ax1 = plt.subplot(gs[band_id])
            hist, xe, ye= np.histogram2d(arr1, arr2, bins=(xedges, yedges))
            plt.pcolor(X,Y,hist, cmap=cmname, norm=LogNorm(1, vmax=hist.max()))
            plt.colorbar()
            if name_s2: ax1.set_xlabel(name_s2)
            if name_ls: ax1.set_ylabel(name_ls)
            ax1.set_title(band)
            ax1.plot([0,100],[0,100])
            ax1.plot(np.arange(0,100,10), regr.predict(np.arange(0,100,10)[:,np.newaxis]), ':')
            ax1.text(10, 90, 'spearmanr = {0:.2f}'.format(sr))
            ax1.text(10, 80, 'rmse = {0:.2f}'.format(rmse))
    
    if savefig:
        if isinstance(savefig, str): plt.savefig(savefig)
        else: plt.savefig('_'.join(title.split(' '))+'.png')
            
    return rmses
