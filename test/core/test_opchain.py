import os
import shutil
from unittest import TestCase
import xarray as xr

from xcube.core.new import new_cube

from esa_climate_toolbox.core.ds import add_local_store
from esa_climate_toolbox.core.ds import open_data
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
        self.add_op = self.registry.get_op("test.core.test_opchain.op_add_test")

        @op(registry=self.registry)
        @op_input('ds', data_type=DatasetLike)
        @op_return(add_history=True)
        def op_multiplier_test(ds: xr.Dataset,
                   multiplier: int = 3) -> xr.Dataset:
            return ds.map(lambda var: var * multiplier if var.dtype.kind in 'if' else var)
        self.multiply_op = self.registry.get_op("test.core.test_opchain.op_multiplier_test")

        root = os.path.join(os.path.dirname(__file__), 'test_chain')
        self._local_store_id = add_local_store(root, persist=False)

        self.cube_1 = new_cube(
            variables=dict(a=2, b=4),
            width=2,
            height=2,
            time_periods=1
        )

        self.cube_2 = new_cube(
            variables=dict(c=13, d=25),
            width=2,
            height=2,
            time_periods=1
        )
        self.cube_1_id = write_data(self.cube_1, store_id=self._local_store_id)
        self.cube_2_id = write_data(self.cube_2, store_id=self._local_store_id)
        os.makedirs("test_chain", exist_ok=True)

    def tearDown(self):
        self.registry = None
        remove_store(self._local_store_id)
        shutil.rmtree("test_chain")

    def test_single_ds_and_op(self):
        res = execute_operations(
            self.cube_1,
            "test.core.test_opchain.op_add_test",
            op_registry=self.registry,
            monitor=MyMonitor()
        )

        self.assertIsNotNone(res)
        self.assertIs(1, len(res))
        self.assertListEqual([3, 3], res[0].a.values[0][0].tolist())
        self.assertListEqual([5, 5], res[0].b.values[0][0].tolist())

    def test_two_ds_and_one_op(self):
        res = execute_operations(
            [self.cube_1_id, self.cube_2],
            self.add_op,
            op_registry=self.registry,
            monitor=MyMonitor()
        )

        self.assertIsNotNone(res)
        self.assertIs(2, len(res))
        self.assertListEqual([3, 3], res[0].a.values[0][0].tolist())
        self.assertListEqual([5, 5], res[0].b.values[0][0].tolist())
        self.assertListEqual([14, 14], res[1].c.values[0][0].tolist())
        self.assertListEqual([26, 26], res[1].d.values[0][0].tolist())

    def test_one_ds_and_two_ops(self):
        res = execute_operations(
            self.cube_2,
            [{"test.core.test_opchain.op_add_test": {
                "adder": 2}
            },
             self.multiply_op],
            op_registry=self.registry,
            monitor=MyMonitor()
        )

        self.assertIsNotNone(res)
        self.assertIs(1, len(res))

        self.assertListEqual([45, 45], res[0].c.values[0][0].tolist())
        self.assertListEqual([81, 81], res[0].d.values[0][0].tolist())

    def test_two_ds_and_two_ops(self):
        res = execute_operations(
            [self.cube_1, self.cube_2],
            [{"test.core.test_opchain.op_add_test": {
                "adder": 2}
            },
            {"test.core.test_opchain.op_multiplier_test": {
                "multiplier": 2}
            }
            ],
            op_registry=self.registry,
            monitor=MyMonitor()
        )

        self.assertIsNotNone(res)
        self.assertIs(2, len(res))

        self.assertListEqual([8, 8], res[0].a.values[0][0].tolist())
        self.assertListEqual([12, 12], res[0].b.values[0][0].tolist())
        self.assertListEqual([30, 30], res[1].c.values[0][0].tolist())
        self.assertListEqual([54, 54], res[1].d.values[0][0].tolist())

    def test_write_to_folder(self):
        res = execute_operations(
            self.cube_1,
            "test.core.test_opchain.op_add_test",
            op_registry=self.registry,
            monitor=MyMonitor(),
            write_results=True,
            output_folder="test_data/",
            output_file_name="add_result.zarr",
            replace_output=True
        )
        self.assertIsNotNone(res)
        self.assertEqual(1, len(res))
        self.assertEqual(2, len(res[0]))
        self.assertEqual("add_result.zarr", res[0][1])
        self.assertEqual(True, os.path.exists("test_data/add_result.zarr"))

    def test_write_to_store(self):
        res = execute_operations(
            self.cube_1,
            "test.core.test_opchain.op_add_test",
            op_registry=self.registry,
            monitor=MyMonitor(),
            write_results=True,
            output_store_id=self._local_store_id,
            output_format="netcdf"
        )
        self.assertIsNotNone(res)
        self.assertEqual(1, len(res))
        self.assertEqual(2, len(res[0]))
        self.assertEqual(True, res[0][1].endswith(".nc"))
        ds_read, _ = open_data(res[0][1], data_store_id=self._local_store_id)
        self.assertIsNotNone(ds_read)
        self.assertListEqual([3, 3], ds_read.a.values[0][0].tolist())
        self.assertListEqual([5, 5], ds_read.b.values[0][0].tolist())

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
