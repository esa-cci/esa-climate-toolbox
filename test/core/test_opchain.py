import os
from unittest import TestCase
import xarray as xr

from xcube.core.new import new_cube

from esa_climate_toolbox.core.ds import add_local_store
from esa_climate_toolbox.core.ds import remove_store
from esa_climate_toolbox.core.ds import write_data
from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import OpRegistry
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.opchain import execute_operations
from esa_climate_toolbox.core.types import DatasetLike
from esa_climate_toolbox.util.monitor import Monitor

class ExecuteOpsTest(TestCase):

    def setUp(self):
        self.registry = OpRegistry()

        @op(registry=self.registry)
        @op_input('ds', data_type=DatasetLike)
        @op_return(add_history=True)
        def op_add_test(ds: xr.Dataset,
                   adder: int = 1,
                   monitor: Monitor = Monitor.NONE) -> xr.Dataset:
            monitor.start("Adding", total_work=len(ds.data_vars))
            for var_name in ds.data_vars:
                if ds[var_name].dtype.kind in 'if':
                    ds[var_name] = ds[var_name] + adder
                    monitor.progress(1)
            monitor.done()
            return ds

        @op(registry=self.registry)
        @op_input('ds', data_type=DatasetLike)
        @op_return(add_history=True)
        def op_divide_test(ds: xr.Dataset,
                   divisor: int = 1) -> xr.Dataset:
            return ds.map(lambda var: var / divisor if var.dtype.kind in 'if' else var)

        root = os.path.join(os.path.dirname(__file__), 'test_data')
        self._local_store_id = add_local_store(root, persist=False)

        self.cube_1 = new_cube(
            variables=dict(a=2.5, b=4.7),
            width=2,
            height=2,
            time_periods=1
        )

        self.cube_2 = new_cube(
            variables=dict(c=13.2, d=25.1),
            width=2,
            height=2,
            time_periods=1
        )
        self.cube_1_id = write_data(self.cube_1, store_id=self._local_store_id)
        self.cube_2_id = write_data(self.cube_2, store_id=self._local_store_id)

    def tearDown(self):
        self.registry = None
        remove_store(self._local_store_id)

    def test_single_ds_and_op(self):
        res = execute_operations(
            self.cube_1,
            "test.core.test_opchain.op_add_test",
            op_registry=self.registry,
            monitor=MyMonitor()
        )

        self.assertIsNotNone(res)
        self.assertIs(1, len(res))
        self.assertListEqual([3.5, 3.5], res[0].a.values[0][0].tolist())
        self.assertListEqual([5.7, 5.7], res[0].b.values[0][0].tolist())


class MyMonitor(Monitor):

    def __init__(self):
        self.total_work = 0
        self.worked = 0
        self.is_done = False

    def start(self, label: str, total_work: float = None):
        self.total_work = total_work

    def progress(self, work: float = None, msg: str = None):
        self.check_for_cancellation()
        self.worked += work

    def done(self):
        self.is_done = True
