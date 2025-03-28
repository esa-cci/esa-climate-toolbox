# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
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

"""
The core API of the ESA Climate Toolbox.
"""

from .ds import add_local_store
from .ds import add_store
from .ds import find_data_store
from .ds import get_output_store_id
from .ds import get_store
from .ds import get_search_params
from .ds import get_supported_formats
from .ds import list_datasets
from .ds import list_ecvs
from .ds import list_ecv_datasets
from .ds import list_stores
from .ds import open_data
from .ds import remove_store
from .ds import search
from .ds import set_output_store
from .ds import write_data

# noinspection PyUnresolvedReferences
from .op import list_operations
from .op import get_op
from .op import get_op_meta_info
from .op import new_expression_op
from .op import new_subprocess_op
from .op import op
from .op import op_input
from .op import op_output
from .op import op_return
from .op import Operation
from .op import OP_REGISTRY

from .opchain import execute_operations

# noinspection PyUnresolvedReferences
from ..util.monitor import Monitor

# noinspection PyUnresolvedReferences
from ..util.monitor import ChildMonitor

# noinspection PyUnresolvedReferences
from ..util.opmetainf import OpMetaInfo

# Run plugin registration by importing the plugin module
# noinspection PyUnresolvedReferences
from .plugin import ect_init as _

del _
