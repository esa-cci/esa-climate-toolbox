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

from typing import Any
from typing import Container
from typing import Dict
from typing import Iterator
from typing import Tuple
from typing import Union

import json

from xcube.core.store import DataStoreError
from xcube.core.store import DataTypeLike
from xcube.core.store import get_data_store_class
from xcube.util.jsonschema import JsonObjectSchema

CCI_ZARR_STORE_BUCKET_NAME = 'esacci'
CCI_ZARR_STORE_ENDPOINT = 'https://cci-ke-o.s3-ext.jc.rl.ac.uk:443/'
DATA_IDS_FILE_PATH = f'{CCI_ZARR_STORE_BUCKET_NAME}/data_ids.json'

CCI_ZARR_STORE_PARAMS = dict(
    root=CCI_ZARR_STORE_BUCKET_NAME,
    storage_options=dict(
        anon=True,
        client_kwargs=dict(
            endpoint_url=CCI_ZARR_STORE_ENDPOINT,
        )
    )
)

S3DataStore = get_data_store_class('s3')


class CciZarrDataStore(S3DataStore):

    def __init__(self):
        super().__init__(**CCI_ZARR_STORE_PARAMS)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        return JsonObjectSchema(additional_properties=False)

    def get_data_ids(self,
                     data_type: DataTypeLike = None,
                     include_attrs: Container[str] = None) -> \
            Union[Iterator[str], Iterator[Tuple[str, Dict[str, Any]]]]:
        # TODO: do not ignore names in include_attrs
        if self.fs.exists(DATA_IDS_FILE_PATH):
            return_tuples = include_attrs is not None
            with self.fs.open(DATA_IDS_FILE_PATH) as f:
                data_ids = json.load(f)
                for data_id in data_ids:
                    yield (data_id, {}) if return_tuples else data_id
        else:
            yield from super().get_data_ids(
                data_type=data_type,
                include_attrs=include_attrs
            )

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def get_data_writer_ids(self, data_type: DataTypeLike = None) -> \
            Tuple[str, ...]:
        return ()

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def get_write_data_params_schema(self, **kwargs) -> \
            JsonObjectSchema:
        return JsonObjectSchema(additional_properties=False)

    def write_data(self, *args, **kwargs) -> str:
        raise DataStoreError('The CciZarrDataStore is read-only.')

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def get_delete_data_params_schema(self, **kwargs) -> \
            JsonObjectSchema:
        return JsonObjectSchema(additional_properties=False)

    def delete_data(self, *args):
        raise DataStoreError('The CciZarrDataStore is read-only.')
