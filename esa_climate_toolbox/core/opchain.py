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

from typing import Any
from typing import List
from typing import Mapping
from typing import Union

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


def execute_ops(
    operation_inputs: Union[INPUT_TYPE, List[INPUT_TYPE]],
    operations: Union[OPERATION_TYPE, Mapping[OPERATION_TYPE, Mapping[str, Any]]],
    op_registry: OpRegistry = OP_REGISTRY,
    monitor: Monitor = None,
    write_results: bool = False,
    output_folder: str = None,
    output_store_id: str = None,
    output_file_name: str = None,
    output_format: str = None,
    replace_output: bool = False,
):
    op_input_data = []
    for op_input in operation_inputs:
        if isinstance(str, op_input):
            op_input_data.append(open_data(dataset_id=op_input))
        elif isinstance(Mapping, op_input):
            for ds_name, open_params_dict in op_input.items():
                op_input_data.append(open_data(ds_name, **open_params_dict))
        elif isinstance(DatasetLike, op_input):
            op_input_data.append(op_input)
        else:
            raise ValueError(f"Cannot determine input '{str(op_input)}'.")

    actual_operations = {}
    for operation in operations:
        if isinstance(str, operation):
            actual_operations[op_registry.get_op(operation)] = {}
        elif isinstance(Operation, operation):
            actual_operations[operation] = {}
        elif isinstance(Mapping, operation):
            for op, op_params in operation.items():
                if isinstance(str, op):
                    actual_operations[op_registry.get_op(operation)] = op_params
                elif isinstance(Operation, op):
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

    for op_input_ds in op_input_data:
        current_ds = op_input_ds
        for actual_operation, ao_params in actual_operations.items():
            if actual_operation.op_meta_info.has_monitor:
                current_ds = actual_operation(current_ds, monitor=monitor, **ao_params)
            else:
                current_ds = actual_operation(current_ds, **ao_params)
        if write_results:
            data_id = write_data(
                current_ds, data_id=output_file_name, store_id=output_store_id,
                format_id=output_format, replace=replace_output, monitor=monitor
            )
            chain_results.append(tuple[current_ds, data_id])
        else:
            chain_results.append(current_ds)

    return chain_results
