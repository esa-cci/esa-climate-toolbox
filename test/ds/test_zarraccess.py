# The MIT License (MIT)
# Copyright (c) 2021 by the xcube development team and contributors
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

import os
import unittest

from xcube.core.store import DataStoreError
from xcube.util.jsonschema import JsonObjectSchema
from esa_climate_toolbox.ds.zarraccess import CciZarrDataStore


@unittest.skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
                 'ECT_DISABLE_WEB_TESTS = 1')
class CciZarrDataStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CciZarrDataStore()

    def test_get_data_store_params_schema(self):
        data_store_params_schema = self.store.get_data_store_params_schema()
        self.assertIsNotNone(data_store_params_schema)
        self.assertEqual(
            JsonObjectSchema(additional_properties=False).to_dict(),
            data_store_params_schema.to_dict()
        )

    def test_get_data_types(self):
        self.assertEqual({'mldataset', 'geodataframe', 'dataset'},
                         set(self.store.get_data_types()))

    def test_get_data_opener_ids(self):
        self.assertEqual({'dataset:geotiff:s3',
                          'dataset:netcdf:s3',
                          'dataset:zarr:s3',
                          'dataset:levels:s3',
                          'mldataset:geotiff:s3',
                          'mldataset:levels:s3',
                          'geodataframe:shapefile:s3',
                          'geodataframe:geojson:s3'},
                         set(self.store.get_data_opener_ids()))
        self.assertEqual({'dataset:geotiff:s3',
                          'dataset:netcdf:s3',
                          'dataset:zarr:s3',
                          'dataset:levels:s3'},
                         set(self.store.get_data_opener_ids(
                             data_type='dataset'
                         )))
        self.assertEqual({'dataset:geotiff:s3',
                          'dataset:netcdf:s3',
                          'dataset:zarr:s3',
                          'dataset:levels:s3',
                          'mldataset:geotiff:s3',
                          'mldataset:levels:s3',
                          'geodataframe:shapefile:s3',
                          'geodataframe:geojson:s3'},
                         set(self.store.get_data_opener_ids(
                             data_type='*'
                         )))
        self.assertEqual((), self.store.get_data_opener_ids(data_type=int))

    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema()
        self.assertEqual(
            {'log_access',
             'cache_size',
             'chunks',
             'consolidated',
             'decode_cf',
             'decode_coords',
             'decode_times',
             'drop_variables',
             'group',
             'mask_and_scale'},
            set(schema.properties.keys())
        )
        self.assertEqual([], schema.required)

    def test_get_data_writer_ids(self):
        self.assertEqual((), self.store.get_data_writer_ids())

    def test_get_write_data_params_schema(self):
        self.assertEqual(
            JsonObjectSchema(additional_properties=False).to_dict(),
            self.store.get_write_data_params_schema().to_dict()
        )

    def test_write_data(self):
        import xarray as xr
        data = xr.Dataset()
        with self.assertRaises(DataStoreError) as dse:
            self.store.write_data(data)
        self.assertEqual('The CciZarrDataStore is read-only.',
                         f'{dse.exception}')

    def test_delete_data(self):
        with self.assertRaises(DataStoreError) as dse:
            self.store.delete_data('some_data_id')
        self.assertEqual('The CciZarrDataStore is read-only.',
                         f'{dse.exception}')
