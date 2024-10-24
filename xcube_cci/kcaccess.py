# The MIT License (MIT)
# Copyright (c) 2023-2024 ESA Climate Change Initiative
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

import json
import os.path

from xcube.core.store import get_data_store_class

ReferenceDataStore = get_data_store_class('reference')


class CciKerchunkDataStore(ReferenceDataStore):
    _kc_refs = None

    def __init__(self):
        if self._kc_refs is None:
            kc_refs_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'data/kc_refs.json'
            )
            with open(kc_refs_path, 'r') as fp:
                self._kc_refs = json.load(fp)
        super().__init__(self._kc_refs, target_options=dict(compression=None))
