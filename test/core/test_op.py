import os.path
import sys
from collections import OrderedDict
from unittest import TestCase

import xarray as xr

from esa_climate_toolbox.core.op import list_operations
from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_output
from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.core.op import OpRegistry
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.op import new_expression_op
from esa_climate_toolbox.core.op import new_subprocess_op
from esa_climate_toolbox.core.types import FileLike
from esa_climate_toolbox.core.types import VarName
from esa_climate_toolbox.util.misc import object_to_qualified_name
from esa_climate_toolbox.util.monitor import Monitor
from esa_climate_toolbox.util.opmetainf import OpMetaInfo

MONITOR = OpMetaInfo.MONITOR_INPUT_NAME
RETURN = OpMetaInfo.RETURN_OUTPUT_NAME

DIR = os.path.dirname(__file__)
SOILMOISTURE_NC = os.path.join(
    DIR, 'test_data', 'small',
    'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc'
)

MAKE_ENTROPY_EXE = sys.executable + " " + \
                   os.path.join(DIR, 'executables', 'mkentropy.py')
FILTER_DS_EXE = sys.executable + " " + \
                os.path.join(DIR, 'executables', 'filterds.py')


class OpTest(TestCase):

    def setUp(self):
        self.registry = OpRegistry()

    def tearDown(self):
        self.registry = None

    def test_list_operations(self):
        self.assertEqual(0, len(list_operations(self.registry)))

        @op(tags=["LC"], registry=self.registry)
        @op_input('a', value_range=[0., 1.], registry=self.registry)
        @op_return(registry=self.registry)
        def f_op_inp_ret_list_test(a: float, w=4.9) -> str:
            return str(a + w)

        self.assertEqual(1, len(list_operations(self.registry)))
        self.assertEqual(0, len(list_operations(self.registry, tag="LAKES")))
        self.assertEqual(1, len(list_operations(self.registry, tag="LC")))
        self.registry.remove_op(f_op_inp_ret_list_test)


    def test_new_executable_op_without_ds(self):
        sub_op = new_subprocess_op(
            OpMetaInfo(
                'make_entropy',
                inputs={
                    'num_steps': {'data_type': int},
                    'period': {'data_type': float},
                },
                outputs={
                    'return': {'data_type': int}
                }
            ),
            MAKE_ENTROPY_EXE + " {num_steps} {period}"
        )
        exit_code = sub_op(num_steps=5, period=0.05)
        self.assertEqual(exit_code, 0)

    def test_new_executable_op_with_ds_file(self):
        sub_op = new_subprocess_op(
            OpMetaInfo(
                'filter_ds',
                inputs={
                    'in_file': {'data_type': FileLike},
                    'out_file': {'data_type': FileLike},
                    'var': {'data_type': VarName},
                },
                outputs={
                    'return': {'data_type': int}
                }),
            FILTER_DS_EXE + " {in_file} {out_file} {var}"
        )
        ofile = os.path.join(DIR, 'test_data', 'filter_ds.nc')
        if os.path.isfile(ofile):
            os.remove(ofile)
        exit_code = sub_op(in_file=SOILMOISTURE_NC, out_file=ofile, var='sm')
        self.assertEqual(exit_code, 0)
        self.assertTrue(os.path.isfile(ofile))
        os.remove(ofile)

    def test_new_executable_op_with_ds_in_mem(self):
        sub_op = new_subprocess_op(
            OpMetaInfo(
                'filter_ds',
                inputs={
                    'ds': {
                        'data_type': xr.Dataset,
                        'write_to': 'in_file'
                    },
                    'var': {'data_type': VarName},
                },
                outputs={
                    'return': {
                        'data_type': xr.Dataset,
                        'read_from': 'out_file'
                    }
                }),
            FILTER_DS_EXE + " {in_file} {out_file} {var}")
        ds = xr.open_dataset(SOILMOISTURE_NC)
        ds_out = sub_op(ds=ds, var='sm')
        self.assertIsNotNone(ds_out)
        self.assertIsNotNone('sm' in ds_out)

    def test_new_expression_op(self):
        expression_op = new_expression_op(
            OpMetaInfo('add_xy',
                       inputs={
                           'x': {'data_type': float},
                           'y': {'data_type': float},
                       },
                       outputs={
                           'return': {'data_type': float}
                       }),
            'x + y'
        )
        z = expression_op(x=1.2, y=2.4)
        self.assertEqual(z, 1.2 + 2.4)

        expression_op = new_expression_op(
            OpMetaInfo(
                'add_xy',
                inputs={'x': {}, 'y': {}}),
            'x * y'
        )
        z = expression_op(x=1.2, y=2.4)
        self.assertEqual(z, 1.2 * 2.4)
        self.assertIn('return', expression_op.op_meta_info.outputs)

    def test_plain_function(self):
        def f(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f!"""
            return str(a + b + c + u + len(v) + w)

        registry = self.registry
        added_op_reg = registry.add_op(f)
        self.assertIsNotNone(added_op_reg)

        with self.assertRaises(ValueError):
            registry.add_op(f, fail_if_exists=True)

        self.assertIs(registry.add_op(f, fail_if_exists=False), added_op_reg)

        op_reg = registry.get_op(object_to_qualified_name(f))
        self.assertIs(op_reg, added_op_reg)
        self.assertIs(op_reg.wrapped_op, f)
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(position=3, default_value=3, data_type=int)
        expected_inputs['v'] = dict(
            position=4, default_value='A', data_type=str
        )
        expected_inputs['w'] = dict(
            position=5, default_value=4.9, data_type=float
        )
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f),
                             dict(description='Hi, I am f!'),
                             expected_inputs,
                             expected_outputs)

        removed_op_reg = registry.remove_op(f)
        self.assertIs(removed_op_reg, op_reg)
        op_reg = registry.get_op(object_to_qualified_name(f))
        self.assertIsNone(op_reg)

        with self.assertRaises(ValueError):
            registry.remove_op(f, fail_if_not_exists=True)

    def test_decorated_function(self):
        @op(registry=self.registry)
        def f_op(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f_op!"""
            return str(a + b + c + u + len(v) + w)

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(f_op, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(f_op))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(
            position=3, default_value=3, data_type=int
        )
        expected_inputs['v'] = dict(
            position=4, default_value='A', data_type=str
        )
        expected_inputs['w'] = dict(
            position=5, default_value=4.9, data_type=float
        )
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op),
                             dict(description='Hi, I am f_op!'),
                             expected_inputs,
                             expected_outputs)

    def test_decorated_function_with_inputs_and_outputs(self):
        @op_input('a', value_range=[0., 1.], registry=self.registry)
        @op_input('v', value_set=['A', 'B', 'C'], registry=self.registry)
        @op_return(registry=self.registry)
        def f_op_inp_ret(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f_op_inp_ret!"""
            return str(a + b + c + u + len(v) + w)

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(f_op_inp_ret, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(f_op_inp_ret))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(
            position=0, data_type=float, value_range=[0., 1.]
        )
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(position=3, default_value=3, data_type=int)
        expected_inputs['v'] = dict(
            position=4, default_value='A', data_type=str,
            value_set=['A', 'B', 'C']
        )
        expected_inputs['w'] = dict(
            position=5, default_value=4.9, data_type=float
        )
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op_inp_ret),
                             dict(description='Hi, I am f_op_inp_ret!'),
                             expected_inputs,
                             expected_outputs)

    def _assertMetaInfo(self, op_meta_info: OpMetaInfo,
                        expected_name: str,
                        expected_header: dict,
                        expected_input: OrderedDict,
                        expected_output: OrderedDict):
        self.assertIsNotNone(op_meta_info)
        self.assertEqual(op_meta_info.qualified_name, expected_name)
        self.assertEqual(op_meta_info.header, expected_header)
        self.assertEqual(OrderedDict(op_meta_info.inputs), expected_input)
        self.assertEqual(OrderedDict(op_meta_info.outputs), expected_output)

    def test_function_validation(self):
        @op_input('x', registry=self.registry, data_type=float,
                  value_range=[0.1, 0.9], default_value=0.5)
        @op_input('y', registry=self.registry)
        @op_input('a', registry=self.registry, data_type=int,
                  value_set=[1, 4, 5])
        @op_return(registry=self.registry, data_type=float)
        def f(x, y: float or None, a=4):
            return a * x + y if a != 5 else 'foo'

        self.assertIs(f, self.registry.get_op(f))
        self.assertEqual(
            f.op_meta_info.inputs['x'].get('data_type', None), float
        )
        self.assertEqual(
            f.op_meta_info.inputs['x'].get('value_range', None), [0.1, 0.9]
        )
        self.assertEqual(
            f.op_meta_info.inputs['x'].get('default_value', None), 0.5
        )
        self.assertEqual(
            f.op_meta_info.inputs['x'].get('position', None), 0
        )
        self.assertEqual(
            f.op_meta_info.inputs['y'].get('data_type', None), float
        )
        self.assertEqual(
            f.op_meta_info.inputs['y'].get('position', None), 1
        )
        self.assertEqual(
            f.op_meta_info.inputs['a'].get('data_type', None), int
        )
        self.assertEqual(
            f.op_meta_info.inputs['a'].get('value_set', None), [1, 4, 5]
        )
        self.assertEqual(
            f.op_meta_info.inputs['a'].get('default_value', None), 4
        )
        self.assertEqual(
            f.op_meta_info.inputs['a'].get('position', None), 2
        )
        self.assertEqual(
            f.op_meta_info.outputs[RETURN].get('data_type', None), float
        )

        self.assertEqual(f(y=1, x=0.2), 4 * 0.2 + 1)
        self.assertEqual(f(y=3), 4 * 0.5 + 3)
        self.assertEqual(f(0.6, y=3, a=1), 1 * 0.6 + 3.0)

        with self.assertRaises(ValueError) as cm:
            f(y=1, x=8)
        self.assertEqual(
            str(cm.exception),
            "Input 'x' for operation 'test.core.test_op.f' "
            "must be in range [0.1, 0.9]."
        )

        with self.assertRaises(ValueError) as cm:
            f(y=None, x=0.2)
        self.assertEqual(
            str(cm.exception),
            "Input 'y' for operation 'test.core.test_op.f' must be given."
        )

        with self.assertRaises(ValueError) as cm:
            f(y=0.5, x=0.2, a=2)
        self.assertEqual(
            str(cm.exception),
            "Input 'a' for operation 'test.core.test_op.f' "
            "must be one of [1, 4, 5]."
        )

        with self.assertRaises(ValueError) as cm:
            f(x=0, y=3.)
        self.assertEqual(
            str(cm.exception),
            "Input 'x' for operation 'test.core.test_op.f' "
            "must be in range [0.1, 0.9]."
        )

        with self.assertRaises(ValueError) as cm:
            f(x='A', y=3.)
        self.assertEqual(
            str(cm.exception),
            "Input 'x' for operation 'test.core.test_op.f' "
            "must be of type 'float', but got type 'str'."
        )

        with self.assertRaises(ValueError) as cm:
            f(x=0.4)
        self.assertEqual(
            str(cm.exception),
            "Input 'y' for operation 'test.core.test_op.f' must be given."
        )

        with self.assertRaises(ValueError) as cm:
            f(x=0.6, y=0.1, a=2)
        self.assertEqual(
            str(cm.exception),
            "Input 'a' for operation 'test.core.test_op.f' "
            "must be one of [1, 4, 5]."
        )

        with self.assertRaises(ValueError) as cm:
            f(y=3, a=5)
        self.assertEqual(
            str(cm.exception),
            "Output 'return' for operation 'test.core.test_op.f' "
            "must be of type 'float', but got type 'str'."
        )

    def test_function_invocation(self):
        def f(x, a=4):
            return a * x

        op_reg = self.registry.add_op(f)
        result = op_reg(x=2.5)
        self.assertEqual(result, 4 * 2.5)

    def test_function_invocation_with_monitor(self):
        def f(m: Monitor, x, a=4):
            m.start('f', 23)
            return_value = a * x
            m.done()
            return return_value

        op_reg = self.registry.add_op(f)
        monitor = MyMonitor()
        result = op_reg(x=2.5, m=monitor)
        self.assertEqual(result, 4 * 2.5)
        self.assertEqual(monitor.total_work, 23)
        self.assertEqual(monitor.is_done, True)

    def test_history_op(self):
        """
        Test adding operation signature to output history information.
        """
        from esa_climate_toolbox import __version__

        # Test @op_return
        @op(version='0.9', registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_op(dataset: xr.Dataset, a=1, b='bilinear'):
            res = dataset.copy()
            return res

        ds = xr.Dataset()

        op_reg = self.registry.get_op(object_to_qualified_name(history_op))
        op_meta_info = op_reg.op_meta_info

        # This is a partial stamp, as the way a dict is stringified is not
        # always the same
        stamp = '\nModified with the ESA Climate Toolbox v' \
                + __version__ + ' ' + op_meta_info.qualified_name + ' v' + \
                op_meta_info.header['version'] + \
                ' \nDefault input values: ' + \
                str(op_meta_info.inputs) + '\nProvided input values: '

        ret_ds = op_reg(dataset=ds, a=2, b='trilinear')
        self.assertTrue(stamp in ret_ds.attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('trilinear' in ret_ds.attrs['history'])

        # Double line-break indicates that this is a subsequent stamp entry
        stamp2 = '\n\nModified with the ESA Climate Toolbox v' + __version__

        ret_ds = op_reg(dataset=ret_ds, a=4, b='quadrilinear')
        self.assertTrue(stamp2 in ret_ds.attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('quadrilinear' in ret_ds.attrs['history'])
        # Check that a previous passed value is found in the stamp
        self.assertTrue('trilinear' in ret_ds.attrs['history'])

        # Test @op_output
        @op(version='1.9', registry=self.registry)
        @op_output('name1', add_history=True, registry=self.registry)
        @op_output('name2', add_history=False, registry=self.registry)
        @op_output('name3', registry=self.registry)
        def history_named_op(dataset: xr.Dataset, a=1, b='bilinear'):
            ds1 = dataset.copy()
            ds2 = dataset.copy()
            ds3 = dataset.copy()
            return {'name1': ds1, 'name2': ds2, 'name3': ds3}

        ds = xr.Dataset()

        op_reg = self.registry.get_op(
            object_to_qualified_name(history_named_op)
        )
        op_meta_info = op_reg.op_meta_info

        # This is a partial stamp, as the way a dict is stringified is not
        # always the same
        stamp = '\nModified with the ESA Climate Toolbox v' \
                + __version__ + ' ' + op_meta_info.qualified_name + ' v' + \
                op_meta_info.header['version'] + \
                ' \nDefault input values: ' + \
                str(op_meta_info.inputs) + '\nProvided input values: '

        ret = op_reg(dataset=ds, a=2, b='trilinear')
        # Check that the dataset was stamped
        self.assertTrue(stamp in ret['name1'].attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('trilinear' in ret['name1'].attrs['history'])
        # Check that none of the other two datasets have been stamped
        with self.assertRaises(KeyError):
            ret['name2'].attrs['history']
        with self.assertRaises(KeyError):
            ret['name3'].attrs['history']

        # Double line-break indicates that this is a subsequent stamp entry
        stamp2 = '\n\nModified with the ESA Climate Toolbox v' + __version__

        ret = op_reg(dataset=ret_ds, a=4, b='quadrilinear')
        self.assertTrue(stamp2 in ret['name1'].attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('quadrilinear' in ret['name1'].attrs['history'])
        # Check that a previous passed value is found in the stamp
        self.assertTrue('trilinear' in ret['name1'].attrs['history'])
        # Other datasets should have the old history, while 'name1' should be
        # updated
        self.assertTrue(ret['name1'].attrs['history']
                        != ret['name2'].attrs['history'])
        self.assertTrue(ret['name1'].attrs['history']
                        != ret['name3'].attrs['history'])
        self.assertTrue(ret['name2'].attrs['history']
                        == ret['name3'].attrs['history'])

        # Test missing version
        @op(registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_no_version(dataset: xr.Dataset, a=1, b='bilinear'):
            dataset1 = dataset.copy()
            return dataset1

        ds = xr.Dataset()

        op_reg = self.registry.get_op(
            object_to_qualified_name(history_no_version)
        )
        with self.assertRaises(ValueError) as err:
            op_reg(dataset=ds, a=2, b='trilinear')
        self.assertTrue('Could not add history' in str(err.exception))

        # Test not implemented output type stamping
        @op(version='1.1', registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_wrong_type(ds: xr.Dataset, a=1, b='bilinear'):
            return "Joke's on you"

        ds = xr.Dataset()
        op_reg = self.registry.get_op(
            object_to_qualified_name(history_wrong_type)
        )
        with self.assertRaises(NotImplementedError) as err:
            op_reg(ds=ds, a=2, b='abc')
        self.assertTrue(
            'Adding history information to an' in str(err.exception)
        )


class DefaultOpRegistryTest(TestCase):
    def test_it(self):
        self.assertIsNotNone(OP_REGISTRY)
        self.assertEqual(repr(OP_REGISTRY), 'OP_REGISTRY')


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
