# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import geopandas as gpd
from typing import Any
from typing import List
from typing import Mapping
from typing import Union
import xarray as xr

from .ds import add_local_store
from .ds import open_data
from .ds import write_data
from .op import Operation
from .op import OpRegistry
from .op import OP_REGISTRY
from .types import DatasetLike, DictLike
from esa_climate_toolbox.util.monitor import Monitor

INPUT_TYPE = Union[str, DatasetLike, Mapping[str, Mapping[str, Any]]]
OPERATION_TYPE = Union[str, Operation]
OPERATIONS_TYPE = Union[OPERATION_TYPE, Mapping[OPERATION_TYPE, Mapping[str, Any]]]


def execute_operations(
    operation_inputs: Union[INPUT_TYPE, List[INPUT_TYPE]],
    operations: Union[OPERATIONS_TYPE, List[OPERATIONS_TYPE]],
    op_registry: OpRegistry = OP_REGISTRY,
    monitor: Monitor = Monitor.NONE,
    write_results: bool = False,
    output_folder: str = None,
    output_store_id: str = None,
    output_file_name: str = None,
    output_format: str = None,
    replace_output: bool = False,
):
    """
    A function that allows to execute multiple operators consecutively, effectively
    forming an operator chain. It may be run on one or more inputs. If run on
    multiple datasets, the chain will be applied to each input separately.

    :param operation_inputs: Either a single input dataset / geodataframe or a list of
        datasets / geodataframes. It is also possible to only pass in a dataset id,
        and the function will look up the corresponding dataset from the stores
        (this will not work when the id is ambiguous). Lastly, one may also pass in
        a dictionary, where the key is a dataset id, and the values are parameters for
        opening a dataset.
        In a list, datasets, geodataframes and data ids may be mixed.
    :param operations: A list of single operations, names of operations, or dictionaries,
        mapping an operation or operation name to a dictionary of operation parameters.
        The list may be freely mixed of these types. It is also possible to pass in a single
        operation, operation name, or dictionary as described above, without the need to
        put it in a list.
    :param op_registry: An optional OpRegistry, in case the default one should
        not be used.
    :param monitor: An optional progress monitor
    :param write_results: Whether the output of each application of the operation chain shall
        be written to disk. If True, the results will be written and the data_ids will be
        returned. False by default.
    :param output_folder: A folder to which the data shall be written. Is only considered when
        write_results is True. Must not be set if output_store_id is used.
    :param output_store_id: The id of a store that shall be used to write the processing results.
        Is only considered when write_results is True. Must not be set if output_folder is used.
    :param output_file_name: The name the output file shall. Is only considered when
        write_results is True and when there is exactly one output.
    :param output_format: The format the output files shall have. Is only considered when
        write_results is True.
    :param replace_output: Whether to replace potentially existing output. Is only considered
        when write_results is True
    :return: Either a list of processing results (one per input), or, when write_results is True,
        a list of tuples of processing results and their data ids.
    """
    if not isinstance(operation_inputs, List):
        operation_inputs = [operation_inputs]
    op_input_data = []
    for op_input in operation_inputs:
        if isinstance(op_input, str):
            op_input_data.append(open_data(dataset_id=op_input)[0])
        elif isinstance(op_input, xr.Dataset) or isinstance(op_input, gpd.GeoDataFrame):
            op_input_data.append(op_input)
        elif isinstance(op_input, Mapping):
            for ds_name, open_params_dict in op_input.items():
                op_input_data.append(open_data(ds_name, **open_params_dict))
        else:
            raise ValueError(f"Cannot determine input '{str(op_input)}'.")

    if not isinstance(operations, List):
        operations = [operations]
    actual_operations = {}
    for operation in operations:
        if isinstance(operation, str):
            actual_operations[op_registry.get_op(operation)] = {}
        elif isinstance(operation, Operation):
            actual_operations[operation] = {}
        elif isinstance(operation, Mapping):
            for op, op_params in operation.items():
                if isinstance(op, str):
                    actual_operations[op_registry.get_op(op)] = op_params
                elif isinstance(op, Operation):
                    actual_operations[operation] = op_params
                else:
                    raise ValueError(f"Cannot determine operation '{str(op)}'")
        else:
            raise ValueError(f"Cannot determine operation '{str(operation)}'")

    if write_results:
        if output_folder is not None:
            if output_store_id is not None:
                raise ValueError(
                    "Only one of parameters 'output_folder' and 'output_store' can be set."
                )
            output_store_id = add_local_store(output_folder)
        if len(operation_inputs) > 1 and output_file_name is not None:
            raise ValueError(
                "Parameter 'output_file_name' is only supported for chains with a single input."
            )

    chain_results = []

    work_steps = len(op_input_data) * len(actual_operations) * 10
    monitor.start("Starting operation chain", work_steps)
    for op_input_ds in op_input_data:
        current_ds = op_input_ds
        for actual_operation, ao_params in actual_operations.items():
            if actual_operation.op_meta_info.has_monitor:
                current_ds = actual_operation(current_ds, monitor=monitor.child(10), **ao_params)
            else:
                current_ds = actual_operation(current_ds, **ao_params)
                monitor.progress(10)
        if write_results:
            data_id = write_data(
                current_ds, data_id=output_file_name, store_id=output_store_id,
                format_id=output_format, replace=replace_output, monitor=monitor
            )
            chain_results.append((current_ds, data_id))
        else:
            chain_results.append(current_ds)
    monitor.done()

    return chain_results
