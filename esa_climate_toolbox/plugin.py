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

from xcube.constants import EXTENSION_POINT_DATA_STORES
from xcube.util import extension

from esa_climate_toolbox.constants import CDC_LONG_DATA_STORE_ID
from esa_climate_toolbox.constants import CDC_SHORT_DATA_STORE_ID


def init_plugin(ext_registry: extension.ExtensionRegistry):
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=CDC_SHORT_DATA_STORE_ID,
        description='ESA Climate Data Centre')
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=CDC_LONG_DATA_STORE_ID,
        description='ESA Climate Data Centre')
