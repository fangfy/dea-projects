import numpy as np
import xarray
import functools

from datacube.storage.masking import create_mask_value
from datacube_stats.incremental_stats import (mk_incremental_sum, mk_incremental_or,
                                              compose_proc, broadcast_proc)
from datacube_stats.utils import first_var

from datacube_stats.statistics import Statistic
from datacube.model import Measurement


class MaskValueCounter(Statistic):
    # pylint: disable=redefined-builtin
    def __init__(self, vars, nodata_value=None):
        """
        vars:
           - name: <output_variable_name: String>
             simple: <optional Bool, default: False>
             equal: 
             not_equal: <only used if equal value is not set>

        # optional, define input nodata as a mask
        # when all inputs match this, then output will be set to nodata
        # this allows to distinguish 0 from nodata

        nodata_value: 0

        If variable is marked simple, then there is no distinction between 0 and nodata.
        """
        self._vars = [v.copy() for v in vars]
        self._nodata_value = nodata_value
        self._valid_pq_mask = None

    def measurements(self, input_measurements):
        nodata = -1
        
        if self._nodata_value is not None:
            self._valid_pq_mask = functools.partial(np.not_equal, self._nodata_value)

        for v in self._vars:
            if 'equal' in v:
                v['mask'] = functools.partial(np.equal, v.get('equal'))
            elif 'not_equal' in v:
                v['mask'] = functools.partial(np.not_equal, v.get('not_equal'))

        return [Measurement(name=v['name'], dtype='int16', units='1', nodata=nodata)
                for v in self._vars]

    def is_iterative(self):
        return True

    def make_iterative_proc(self):
        def _to_mask(ds):
            da = first_var(ds)
            return xarray.Dataset({v['name']: v['mask'](da) for v in self._vars},
                                  attrs=ds.attrs)

        # PQ -> BoolMasks: DataSet<Bool> -> Sum: DataSet<int16>
        proc = compose_proc(_to_mask,
                            proc=mk_incremental_sum(dtype='int16'))

        if self._valid_pq_mask is None:
            return proc

        def invalid_data_mask(da):
            mm = da.values
            if mm.all():  # All pixels had at least one valid observation
                return None
            return np.logical_not(mm, out=mm)

        # PQ -> ValidMask:DataArray<Bool> -> OR:DataArray<Bool> |> InvertMask:ndarray<Bool>|None
        valid_proc = compose_proc(lambda ds: self._valid_pq_mask(first_var(ds)),
                                  proc=mk_incremental_or(),
                                  output_transform=invalid_data_mask)

        _vars = {v['name']: v for v in self._vars}

        def apply_mask(ds, mask, nodata=-1):
            if mask is None:
                return ds

            for name, da in ds.data_vars.items():
                simple = _vars[name].get('simple', False)
                if not simple:
                    da.values[mask] = nodata

            return ds

        # Counts      ----\
        #                  +----------- Counts[ InvalidMask ] = nodata
        # InvalidMask ----/

        return broadcast_proc(proc, valid_proc, combine=apply_mask)

    def compute(self, data):
        proc = self.make_iterative_proc()

        for i in range(data.time.shape[0]):
            proc(data.isel(time=slice(i, i + 1)))

        return proc()

    def __repr__(self):
        return 'MaskValueCounter<{}>'.format(','.join([v['name'] for v in self._vars]))

    
