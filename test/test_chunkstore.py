import numpy
import os
import pandas as pd
import unittest
import xarray as xr

from unittest import skipIf

from xcube_cci.ccicdc import CciCdc
from xcube_cci.chunkstore import CciChunkStore

OC_ID = 'esacci.OC.5-days.L3S.K_490.multi-sensor.multi-platform.MERGED.6-0.' \
        'sinusoidal'
OZONE_ID = 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'
SEAICE_ID = 'esacci.SEAICE.day.L4.SICONC.multi-sensor.multi-platform.' \
            'AMSR_50kmEASE2.2-1.NH'
SST_ID = 'esacci.SST.satellite-orbit-frequency.L3U.SSTskin.AATSR.Envisat.' \
         'AATSR.2-1.r1'


class CciChunkStoreTest(unittest.TestCase):

    @staticmethod
    def _get_test_store():
        cci_cdc = CciCdc()
        dataset_id = OZONE_ID
        time_range = (pd.to_datetime('2010-02-10', utc=True),
                      pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(
            time_range=time_range,
            variable_names=['O3_vmr']
        )
        return CciChunkStore(cci_cdc, dataset_id, cube_params)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_unconstrained_chunk_store(self):
        cci_cdc = CciCdc()
        store = CciChunkStore(cci_cdc, OZONE_ID, cube_params=None)
        self.assertIsNotNone(store)
        time_ranges = store._time_ranges
        self.assertEqual(144, len(time_ranges))
        self.assertEqual(pd.Timestamp('1997-01-01T00:00:00'), time_ranges[0][0])
        self.assertEqual(
            {'surface_pressure', 'O3e_ndens', 'O3e_du', 'O3e_vmr', 'O3e_du_tot',
             'O3_du_tot', 'O3_ndens', 'O3_du', 'O3_vmr'},
            set(store._variable_names))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_chunk_store_with_region_constraint(self):
        cci_cdc = CciCdc()
        cube_params = dict(bbox=[-10, 5, 0, 10])
        store = CciChunkStore(cci_cdc, OZONE_ID, cube_params=cube_params)
        self.assertIsNotNone(store)

        ds = xr.open_zarr(store)

        self.assertEqual(10, ds.lon.size)
        self.assertEqual(10, ds.lon.chunk_sizes)
        self.assertListEqual(
            [-9.5, -8.5, -7.5, -6.5, -5.5, -4.5, -3.5, -2.5, -1.5, -0.5],
            list(ds.lon.values))

        self.assertEqual(5, ds.lat.size)
        self.assertEqual(5, ds.lat.chunk_sizes)
        self.assertListEqual([5.5, 6.5, 7.5, 8.5, 9.5], list(ds.lat.values))

        self.assertEqual(['time', 'layers', 'lat', 'lon'],
                         ds.O3_du.dimensions)
        self.assertEqual((144, 16, 5, 10), ds.O3_du.shape)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_time_ranges(self):
        store = self._get_test_store()
        time_range = (pd.to_datetime('2010-02-10', utc=True),
                      pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = store.get_time_ranges(OZONE_ID, cube_params)
        self.assertEqual([('2010-02-01T00:00:00', '2010-03-01T00:00:00'),
                          ('2010-03-01T00:00:00', '2010-04-01T00:00:00'),
                          ('2010-04-01T00:00:00', '2010-05-01T00:00:00'),
                          ('2010-05-01T00:00:00', '2010-06-01T00:00:00')],
                         [(tr[0].isoformat(), tr[1].isoformat())
                          for tr in time_ranges])
        # get_time_range test for satellite-orbit-frequency data
        time_range = (pd.to_datetime('2002-07-24', utc=True),
                      pd.to_datetime('2002-07-24', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = store.get_time_ranges(SST_ID, cube_params)
        self.assertEqual([('2002-07-24T12:33:21', '2002-07-24T14:13:57'),
                          ('2002-07-24T14:13:57', '2002-07-24T15:54:33'),
                          ('2002-07-24T15:54:33', '2002-07-24T17:35:09'),
                          ('2002-07-24T17:35:09', '2002-07-24T19:15:45'),
                          ('2002-07-24T19:15:45', '2002-07-24T20:56:21'),
                          ('2002-07-24T20:56:21', '2002-07-24T22:36:57'),
                          ('2002-07-24T22:36:57', '2002-07-25T00:17:33')],
                         [(tr[0].isoformat(), tr[1].isoformat())
                          for tr in time_ranges])
        # get_time_range test for data with days-period
        time_range = (pd.to_datetime('2002-07-04', utc=True),
                      pd.to_datetime('2002-07-27', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = store.get_time_ranges(OC_ID, cube_params)
        self.assertEqual([('2002-06-30T00:00:00', '2002-07-04T23:59:00'),
                          ('2002-07-05T00:00:00', '2002-07-09T23:59:00'),
                          ('2002-07-10T00:00:00', '2002-07-14T23:59:00'),
                          ('2002-07-15T00:00:00', '2002-07-19T23:59:00'),
                          ('2002-07-20T00:00:00', '2002-07-24T23:59:00'),
                          ('2002-07-25T00:00:00', '2002-07-29T23:59:00')],
                         [(tr[0].isoformat(), tr[1].isoformat())
                          for tr in time_ranges])
        # get_time_range test for data with day-period
        time_range = (pd.to_datetime('2002-07-04', utc=True),
                      pd.to_datetime('2002-07-09', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = store.get_time_ranges(SEAICE_ID, cube_params)
        self.assertEqual([('2002-07-04T00:00:00', '2002-07-05T00:00:00'),
                          ('2002-07-05T00:00:00', '2002-07-06T00:00:00'),
                          ('2002-07-06T00:00:00', '2002-07-07T00:00:00'),
                          ('2002-07-07T00:00:00', '2002-07-08T00:00:00'),
                          ('2002-07-08T00:00:00', '2002-07-09T00:00:00'),
                          ('2002-07-09T00:00:00', '2002-07-10T00:00:00')],
                         [(tr[0].isoformat(), tr[1].isoformat())
                          for tr in time_ranges])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_dimension_indexes_for_chunk(self):
        store = self._get_test_store()
        dim_indexes = store._get_dimension_indexes_for_chunk(
            'O3_vmr', (5, 0, 0, 0)
        )
        self.assertIsNotNone(dim_indexes)
        self.assertEqual(slice(None, None, None), dim_indexes[0])
        self.assertEqual(slice(0, 17), dim_indexes[1])
        self.assertEqual(slice(0, 180), dim_indexes[2])
        self.assertEqual(slice(0, 360), dim_indexes[3])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_encoding(self):
        store = self._get_test_store()
        encoding_dict = store.get_encoding('surface_pressure')
        self.assertTrue('fill_value' in encoding_dict)
        self.assertTrue('dtype' in encoding_dict)
        self.assertFalse('compressor' in encoding_dict)
        self.assertFalse('order' in encoding_dict)
        self.assertTrue(numpy.isnan(encoding_dict['fill_value']))
        self.assertEqual('float32', encoding_dict['dtype'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_attrs(self):
        store = self._get_test_store()
        attrs = store.get_attrs('surface_pressure')
        self.assertTrue('standard_name' in attrs)
        self.assertTrue('long_name' in attrs)
        self.assertTrue('units' in attrs)
        self.assertTrue('fill_value' in attrs)
        self.assertTrue('chunk_sizes' in attrs)
        self.assertTrue('data_type' in attrs)
        self.assertTrue('dimensions' in attrs)
        self.assertEqual('surface_air_pressure', attrs['standard_name'])
        self.assertEqual('Pressure at the bottom of the atmosphere.',
                         attrs['long_name'])
        self.assertEqual('hPa', attrs['units'])
        self.assertTrue(numpy.isnan(attrs['fill_value']))
        self.assertEqual([1, 180, 360], attrs['chunk_sizes'])
        self.assertEqual('float32', attrs['data_type'])
        self.assertEqual(['time', 'lat', 'lon'], attrs['dimensions'])

    def test_adjust_chunk_sizes(self):
        chunk_sizes = [1, 128, 128]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [2024, 4096, 4096], 0
        )
        self.assertEqual([1, 512, 512], chunk_sizes)

        chunk_sizes = [128, 128, 1]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [4096, 4096, 2048], 2
        )
        self.assertEqual([512, 512, 1], chunk_sizes)

        chunk_sizes = [1, 128, 128]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [2024, 128, 2048], 0
        )
        self.assertEqual([1, 128, 2048], chunk_sizes)

        chunk_sizes = [1, 64, 128, 32]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [2048, 1024, 2048, 1024], 0
        )
        self.assertEqual([1, 64, 128, 32], chunk_sizes)

        chunk_sizes = [1, 90, 180]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [1, 3600, 7200], 0
        )
        self.assertEqual([1, 450, 720], chunk_sizes)
        #
        chunk_sizes = [1, 2048, 2048]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [1, 64800, 129600], 0
        )
        self.assertEqual([1, 2048, 2048], chunk_sizes)

        chunk_sizes = [1, 4096, 4096]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(
            chunk_sizes, [1, 64800, 129600], 0)
        self.assertEqual([1, 2048, 2048], chunk_sizes
                         )

        chunk_sizes = [1]
        chunk_sizes = CciChunkStore._adjust_chunk_sizes(chunk_sizes, [1], -1)
        self.assertEqual([1], chunk_sizes)
