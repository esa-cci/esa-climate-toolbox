"""
Test Utility operations
"""

from datetime import timezone
from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.ops.utility import merge
from esa_climate_toolbox.util.misc import object_to_qualified_name


class MergeTest(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        periods = 5
        time = pd.date_range('2000-01-01', periods=periods, tz=timezone.utc)

        ds_1 = xr.Dataset({'A': (['time'], np.random.randn(periods)),
                           'B': (['time'], np.random.randn(periods)),
                           'time': time})
        ds_2 = xr.Dataset({'C': (['time'], np.random.randn(periods)),
                           'D': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=None, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_2)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_1)
        self.assertIs(new_ds, ds_1)

        new_ds = merge(ds_1=ds_2, ds_2=ds_2, ds_3=ds_2, ds_4=ds_2)
        self.assertIs(new_ds, ds_2)

        ds_3 = xr.Dataset({'E': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

        ds_4 = xr.Dataset({'F': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=ds_4)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

    def test_failures(self):
        with self.assertRaises(ValidationError):
            merge(ds_1=None, ds_2=None, ds_3=None, ds_4=None)
