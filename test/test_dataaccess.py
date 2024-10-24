import datetime as dt
import os
import unittest
from unittest import skip
from unittest import skipIf

from xcube.core.gridmapping import GridMapping
from xcube.core.normalize import normalize_dataset
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataStoreError
from xcube.core.store import GEO_DATA_FRAME_TYPE
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.store.descriptor import GeoDataFrameDescriptor
from xcube.core.verify import assert_cube

from xcube_cci.dataaccess import CciCdcDataStore
from xcube_cci.dataaccess import CciCdcDataFrameOpener
from xcube_cci.dataaccess import CciCdcDatasetOpener
from xcube_cci.dataaccess import get_temporal_resolution_from_id
from xcube_cci.dataaccess import CciCdcVectorDataCubeOpener
from xcube_cci.dataaccess import VectorDataCubeDescriptor
from xcube_cci.dataaccess import VECTOR_DATA_CUBE_TYPE

AEROSOL_DAY_ID = 'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.' \
                 'MSAAI.1-7.r1'
AEROSOL_DAY_ENVISAT_ID = 'esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.' \
                         'ATSR2-ENVISAT-ENS_DAILY.v2-6.r1'
AEROSOL_CLIMATOLOGY_ID = 'esacci.AEROSOL.climatology.L3.AAI.multi-sensor.' \
                         'multi-platform.MSAAI.1-7.r1'
CLOUD_ID = "esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1"
ICESHEETS_ID = 'esacci.ICESHEETS.yr.Unspecified.IV.PALSAR.ALOS.UNSPECIFIED.' \
               '1-1.greenland_margin_2006_2011'
LAKES_ID = 'esacci.LAKES.day.L3S.LK_PRODUCTS.multi-sensor.multi-platform.' \
           'MERGED.v1-1.r1'
OZONE_MON_ID = 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.' \
               'fv0002.r1'
OZONE_MON_GOMOS_ID = 'esacci.OZONE.mon.L3.LP.GOMOS.Envisat.GOMOS_ENVISAT.' \
                     'v0001.r1'
OZONE_MON_SCIAMACHY_ID = 'esacci.OZONE.mon.L3.LP.SCIAMACHY.Envisat.' \
                         'SCIAMACHY_ENVISAT.v0001.r1'
SEAICE_ID = 'esacci.SEAICE.day.L4.SICONC.multi-sensor.multi-platform.' \
            'AMSR_25kmEASE2.2-1.NH'
SST_ID = 'esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1'
GHG_DS_ID = "esacci.GHG.satellite-orbit-frequency.L2.CH4.SCIAMACHY.Envisat.IMAP.v7-2.r1"
VDC_ID = "esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.WAFRICA"
COORDS_2D_ID = "esacci.ICESHEETS.unspecified.L4.SEC.multi-sensor.multi-platform." \
               "UNSPECIFIED.0-1.greenland_sec_saral_altika"


class DataAccessTest(unittest.TestCase):

    def test_get_temporal_resolution_from_id(self):
        self.assertEqual('1D', get_temporal_resolution_from_id(
            'esacci.OZONE.day.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('5D', get_temporal_resolution_from_id(
            'esacci.OZONE.5-days.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('1M', get_temporal_resolution_from_id(
            'esacci.OZONE.mon.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('1M', get_temporal_resolution_from_id(
            'esacci.OZONE.month.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('3M', get_temporal_resolution_from_id(
            'esacci.OZONE.3-months.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('1Y', get_temporal_resolution_from_id(
            'esacci.OZONE.yr.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('1Y', get_temporal_resolution_from_id(
            'esacci.OZONE.year.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('13Y', get_temporal_resolution_from_id(
            'esacci.OZONE.13-yrs.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertEqual('1M', get_temporal_resolution_from_id(
            'esacci.OZONE.climatology.L3.NP.sensor.platform.MERGED.fv0002.r1'
        ))
        self.assertIsNone(get_temporal_resolution_from_id(
            'esacci.OZONE.satellite-orbit-frequency.L3.NP.sensor.platform.'
            'MERGED.fv0002.r1'
        ))


class CciCdcDatasetOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciCdcDatasetOpener(normalize_data=False)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_dataset_names(self):
        self.assertTrue(len(self.opener.dataset_names) > 275)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self.opener.has_data(
            'esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertFalse(self.opener.has_data(
            'esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.'
            '1-7.r1')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.'
            'fv0002.r1')
        )

        self.assertTrue(self.opener.has_data(
            'esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertFalse(self.opener.has_data(
            'esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.'
            '1-7.r1')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.'
            'fv0002.r1')
        )

        self.assertTrue(self.opener.has_data(
            'esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertFalse(self.opener.has_data(
            'esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.'
            '1-7.r1')
        )
        self.assertTrue(self.opener.has_data(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.'
            'fv0002.r1')
        )

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset_crs_variable(self):
        descriptor = self.opener.describe_data(SEAICE_ID)
        self.assertIsNotNone(descriptor)
        self.assertTrue('Lambert_Azimuthal_Grid' in
                        descriptor.data_vars.keys())
        self.assertTrue('xc' in descriptor.coords.keys())
        self.assertTrue('yc' in descriptor.coords.keys())
        self.assertEqual('Lambert Azimuthal Equal Area', descriptor.crs)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_climatology_dataset(self):
        descriptor = self.opener.describe_data(AEROSOL_CLIMATOLOGY_ID)
        self.assertIsNotNone(descriptor)
        self.assertTrue('absorbing_aerosol_index' in
                        descriptor.data_vars.keys())
        self.assertEqual(
            {'longitude', 'latitude', 'month'}, set(descriptor.coords.keys())
        )
        self.assertIsNone(descriptor.time_range)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_data(self):
        descriptor = self.opener.describe_data(OZONE_MON_ID)
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual(OZONE_MON_ID, descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.data_type))
        self.assertEqual(['lon', 'lat', 'layers', 'air_pressure', 'time',
                          'bnds'],
                         list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(16, descriptor.dims['layers'])
        self.assertEqual(17, descriptor.dims['air_pressure'])
        self.assertEqual(36, descriptor.dims['time'])
        self.assertEqual(2, descriptor.dims['bnds'])
        self.assertEqual(9, len(descriptor.data_vars))
        self.assertTrue('surface_pressure' in descriptor.data_vars)
        self.assertEqual(3, descriptor.data_vars['surface_pressure'].ndim)
        self.assertEqual(('time', 'lat', 'lon'),
                         descriptor.data_vars['surface_pressure'].dims)
        self.assertEqual('float32',
                         descriptor.data_vars['surface_pressure'].dtype)
        self.assertEqual('WGS84', descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1997-01-01', '2008-12-31'),
                         descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

        descriptor = self.opener.describe_data(AEROSOL_DAY_ID)
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual(AEROSOL_DAY_ID, descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.data_type))
        self.assertEqual(['longitude', 'latitude', 'time', 'bnds'],
                         list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['longitude'])
        self.assertEqual(180, descriptor.dims['latitude'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(2, descriptor.dims['bnds'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertTrue('absorbing_aerosol_index' in descriptor.data_vars)
        self.assertEqual(3,
                         descriptor.data_vars['absorbing_aerosol_index'].ndim)
        self.assertEqual(('time', 'latitude', 'longitude'),
                         descriptor.data_vars['absorbing_aerosol_index'].dims)
        self.assertEqual('float32',
                         descriptor.data_vars['absorbing_aerosol_index'].dtype)
        self.assertEqual('WGS84', descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'),
                         descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_2d_grid_coords_data(self):
        descriptor = self.opener.describe_data(COORDS_2D_ID)
        self.assertIsNotNone(descriptor)
        self.assertEqual('dataset', str(descriptor.data_type))
        self.assertEqual(['Time', 'x', 'y', 'bnds'], list(descriptor.dims.keys()))
        self.assertEqual(634, descriptor.dims['x'])
        self.assertEqual(635, descriptor.dims['y'])
        self.assertEqual(1, descriptor.dims['Time'])
        self.assertEqual(2, descriptor.dims['bnds'])
        self.assertEqual(7, len(descriptor.data_vars))
        self.assertTrue('CS2SEC' in descriptor.data_vars)
        self.assertEqual(3, descriptor.data_vars['CS2SEC'].ndim)
        self.assertEqual(('y', 'x', 'Time'),
                         descriptor.data_vars['CS2SEC'].dims)
        self.assertEqual('float32',
                         descriptor.data_vars['CS2SEC'].dtype)
        self.assertEqual('Polar Stereographic (variant B)', descriptor.crs)
        self.assertIsNone(descriptor.spatial_res)
        self.assertEqual(('2013-03-31', '2017-03-31'), descriptor.time_range)
        self.assertIsNone(descriptor.time_period)

    def test_get_open_data_params_schema_no_data(self):
        schema = self.opener.get_open_data_params_schema().to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self.opener.get_open_data_params_schema(OZONE_MON_ID).to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

        schema = self.opener.get_open_data_params_schema(AEROSOL_DAY_ID).\
            to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data_sst(self):
        dataset = self.opener.open_data(
            SST_ID,
            variable_names=['analysed_sst'],
            bbox=[-30, 20, -29, 21],
            time_range=['2008-07-01','2008-07-01'])

        self.assertIsNotNone(dataset)
        self.assertEqual({'analysed_sst'}, set(dataset.data_vars))
        self.assertEqual({'lat', 'lat_bnds', 'lon', 'lon_bnds',
                          'time', 'time_bnds'},
                         set(dataset.coords))
        self.assertEqual({'time', 'lat', 'lon'}, set(dataset.analysed_sst.dims))
        self.assertEqual({1, 20, 20}, set(dataset.analysed_sst.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data_aerosol(self):
        dataset = self.opener.open_data(
            AEROSOL_DAY_ID,
            variable_names=['absorbing_aerosol_index'],
            bbox=[40, 40, 50, 50],
            time_range=['2014-01-01','2014-04-30'])
        self.assertIsNotNone(dataset)
        self.assertEqual({'absorbing_aerosol_index'}, set(dataset.data_vars))
        self.assertEqual({'longitude', 'latitude', 'time', 'time_bnds'},
                         set(dataset.coords))
        self.assertEqual({'time', 'latitude', 'longitude'},
                         set(dataset.absorbing_aerosol_index.dims))
        self.assertEqual({1, 10, 10},
                         set(dataset.absorbing_aerosol_index.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

        # open same dataset again to ensure chunking is different
        full_dataset = self.opener.open_data(
            AEROSOL_DAY_ID,
            variable_names=['absorbing_aerosol_index'],
            time_range=['2014-01-01','2014-04-30'])
        self.assertIsNotNone(full_dataset)
        self.assertEqual({'absorbing_aerosol_index'},
                         set(full_dataset.data_vars))
        self.assertEqual({'longitude', 'latitude', 'time', 'time_bnds'},
                         set(full_dataset.coords))
        self.assertEqual({'time', 'latitude', 'longitude'},
                         set(full_dataset.absorbing_aerosol_index.dims))
        self.assertEqual({1, 180, 360},
                         set(full_dataset.absorbing_aerosol_index.chunk_sizes))
        self.assertIsNotNone(full_dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_climatology_data(self):
        dataset = self.opener.open_data(AEROSOL_CLIMATOLOGY_ID)
        self.assertIsNotNone(dataset)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data_with_crs(self):
        dataset = self.opener.open_data(ICESHEETS_ID)
        self.assertIsNotNone(dataset)
        self.assertTrue('crs' in dataset.data_vars.keys())
        self.assertTrue('grid_mapping_name' in dataset['crs'].attrs)

        grid_mapping = GridMapping.from_dataset(dataset)
        self.assertEqual(('x', 'y'), grid_mapping.xy_var_names)
        self.assertEqual((3200, 5400), grid_mapping.size)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while time series are not supported')
    def test_open_data_with_time_series_and_latitude_centers(self):
        dataset = self.opener.open_data(
            OZONE_MON_SCIAMACHY_ID,
            variable_names=['approximate_altitude', 'ozone_mixing_ratio',
                            'sample_standard_deviation'],
            time_range=['2009-05-02', '2009-08-31'],
            bbox=[-10.0, 40.0, 10.0, 60.0]
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio',
                          'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'latitude_centers'},
                         dataset.ozone_mixing_ratio.dims)
        self.assertEqual({1, 32, 18}, dataset.ozone_mixing_ratio.chunk_sizes)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_2d_grid_coords_data(self):
        dataset = self.opener.open_data(
            COORDS_2D_ID,
        )
        self.assertIsNotNone(dataset)
        self.assertEqual(
            {'AltiKaSEC', 'AltiKaSEC_error', 'CS2SEC', 'CS2SEC_error',
             'grid_projection', 'lat', 'lon'},
            set(dataset.data_vars)
        )
        self.assertEqual(('y', 'x', 'Time'), dataset.AltiKaSEC.dims)
        self.assertEqual([635, 634, 1], dataset.AltiKaSEC.chunk_sizes)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search(self):
        search_result = list(self.opener.search_data(cci_attrs=dict(
            ecv='CLOUD',
            product_string='MODIS_TERRA'))
        )
        self.assertIsNotNone(search_result)
        self.assertEqual(1, len(search_result))
        self.assertIsInstance(search_result[0], DatasetDescriptor)
        self.assertEqual(22, len(search_result[0].dims))
        self.assertEqual(147, len(search_result[0].data_vars))
        self.assertEqual(CLOUD_ID, search_result[0].data_id)
        self.assertEqual('1M', search_result[0].time_period)
        self.assertEqual(0.5, search_result[0].spatial_res)
        self.assertEqual(DATASET_TYPE, search_result[0].data_type)
        self.assertEqual(('2000-02-01', '2014-12-31'), search_result[0].time_range)
        self.assertEqual('dataset', search_result[0].data_type.alias)


class CciOdpDatasetOpenerTimeSeriesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciCdcDatasetOpener(normalize_data=False)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_monthly_ozone(self):
        descriptor = self.opener.describe_data(OZONE_MON_GOMOS_ID)
        self.assertIsNotNone(descriptor)
        self.assertEqual('1M', descriptor.time_period)
        self.assertEqual(('2002-01-01', '2011-12-31'), descriptor.time_range)
        self.assertEqual({'time', 'air_pressure', 'latitude_centers', 'bnds'},
                         set(descriptor.dims))
        self.assertEqual(120, descriptor.dims['time'])
        self.assertTrue('time' in descriptor.coords)
        self.assertEqual(120, descriptor.coords['time'].attrs.get('size'))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_monthly_ozone_full(self):
        dataset = self.opener.open_data(
            OZONE_MON_GOMOS_ID,
            variable_names=['approximate_altitude',
                            'ozone_mixing_ratio',
                            'sample_standard_deviation']
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio',
                          'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'latitude_centers'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({120, 51, 18},
                         set(dataset.ozone_mixing_ratio.shape))
        self.assertEqual({12, 51, 18},
                         set(dataset.ozone_mixing_ratio.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_monthly_ozone_time_restricted(self):
        dataset = self.opener.open_data(
            OZONE_MON_GOMOS_ID,
            variable_names=['approximate_altitude',
                            'ozone_mixing_ratio',
                            'sample_standard_deviation'],
            time_range=['2004-05-01', '2004-08-31']
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio',
                          'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'latitude_centers'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({12, 51, 18},
                         set(dataset.ozone_mixing_ratio.shape))
        self.assertEqual({12, 51, 18},
                         set(dataset.ozone_mixing_ratio.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())


class CciOdpDatasetOpenerTimeSeriesNormalizeTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciCdcDatasetOpener(normalize_data=True)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_monthly_ozone(self):
        descriptor = self.opener.describe_data(OZONE_MON_GOMOS_ID)
        self.assertIsNotNone(descriptor)
        self.assertEqual('1M', descriptor.time_period)
        self.assertEqual(('2002-01-01', '2011-12-31'), descriptor.time_range)
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon', 'bnds'},
                         set(descriptor.dims))
        self.assertEqual(120, descriptor.dims['time'])
        self.assertTrue('time' in descriptor.coords)
        self.assertEqual(120, descriptor.coords['time'].attrs.get('size'))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_monthly_ozone_full(self):
        dataset = self.opener.open_data(
            OZONE_MON_GOMOS_ID,
            variable_names=['approximate_altitude',
                            'ozone_mixing_ratio',
                            'sample_standard_deviation']
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio',
                          'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({120, 51, 18, 36},
                         set(dataset.ozone_mixing_ratio.shape))
        self.assertEqual({12, 51, 18, 36},
                         set(dataset.ozone_mixing_ratio.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_monthly_ozone_time_restricted(self):
        dataset = self.opener.open_data(
            OZONE_MON_GOMOS_ID,
            variable_names=['approximate_altitude',
                            'ozone_mixing_ratio',
                            'sample_standard_deviation'],
            time_range=['2004-05-01', '2004-08-31']
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio',
                          'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({12, 51, 18, 36},
                         set(dataset.ozone_mixing_ratio.shape))
        self.assertEqual({12, 51, 18, 36},
                         set(dataset.ozone_mixing_ratio.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())


class CciOdpDatasetOpenerNormalizeTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciCdcDatasetOpener(normalize_data=True)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset_crs_variable(self):
        descriptor = self.opener.describe_data(ICESHEETS_ID)
        self.assertIsNotNone(descriptor)
        self.assertTrue('crs' in descriptor.data_vars.keys())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset(self):
        descriptor = self.opener.describe_data(AEROSOL_DAY_ID)
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual(AEROSOL_DAY_ID, descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.data_type))
        self.assertEqual({'lat', 'lon', 'time', 'bnds'},
                         set(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(2, descriptor.dims['bnds'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertTrue('absorbing_aerosol_index' in descriptor.data_vars)
        self.assertEqual(3,
                         descriptor.data_vars['absorbing_aerosol_index'].ndim)
        self.assertEqual(('time', 'lat', 'lon'),
                         descriptor.data_vars['absorbing_aerosol_index'].dims)
        self.assertEqual('float32',
                         descriptor.data_vars['absorbing_aerosol_index'].dtype)
        self.assertEqual('WGS84', descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'), descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

    def test_get_open_data_params_schema_no_data(self):
        schema = self.opener.get_open_data_params_schema().to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self.opener.get_open_data_params_schema(AEROSOL_DAY_ID).\
            to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        with self.assertRaises(DataStoreError) as dse:
            self.opener.open_data(AEROSOL_DAY_ENVISAT_ID,
                                  variable_names=['AOD550', 'NMEAS'],
                                  time_range=['2009-07-02', '2009-07-05'],
                                  bbox=[-10.0, 40.0, 10.0, 60.0])
        self.assertEqual(
            f'Cannot describe metadata of data resource '
            f'"{AEROSOL_DAY_ENVISAT_ID}", as it cannot be accessed by '
            f'data accessor "dataset:zarr:esa-cdc".', f'{dse.exception}')

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data_aerosol(self):
        dataset = self.opener.open_data(
            AEROSOL_DAY_ID,
            variable_names=['absorbing_aerosol_index'],
            bbox=[40, 40, 50, 50],
            time_range=['2014-01-01','2014-04-30'])
        self.assertIsNotNone(dataset)
        self.assertEqual({'absorbing_aerosol_index'}, set(dataset.data_vars))
        self.assertEqual({'lon', 'lat', 'time', 'time_bnds'},
                         set(dataset.coords))
        self.assertEqual({'time', 'lat', 'lon'},
                         set(dataset.absorbing_aerosol_index.dims))
        self.assertEqual({1, 10, 10},
                         set(dataset.absorbing_aerosol_index.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

        # open same dataset again to ensure chunking is different
        full_dataset = self.opener.open_data(
            AEROSOL_DAY_ID,
            variable_names=['absorbing_aerosol_index'],
            time_range=['2014-01-01','2014-04-30'])
        self.assertIsNotNone(full_dataset)
        self.assertEqual({'absorbing_aerosol_index'},
                         set(full_dataset.data_vars))
        self.assertEqual({'lon', 'lat', 'time', 'time_bnds'},
                         set(full_dataset.coords))
        self.assertEqual({'time', 'lat', 'lon'},
                         set(full_dataset.absorbing_aerosol_index.dims))
        self.assertEqual({1, 180, 360},
                         set(full_dataset.absorbing_aerosol_index.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while time series are not supported')
    def test_open_data_with_time_series_and_latitude_centers(self):
        dataset = self.opener.open_data(
            OZONE_MON_SCIAMACHY_ID,
            variable_names=['standard_error_of_the_mean', 'ozone_mixing_ratio',
                            'sample_standard_deviation'],
            time_range=['2009-05-02', '2009-08-31'],
            bbox=[-10.0, 40.0, 10.0, 60.0]
        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'standard_error_of_the_mean', 'ozone_mixing_ratio',
                          'sample_standard_deviation'}, set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon'},
                         dataset.ozone_mixing_ratio.dims)
        self.assertEqual({1, 32, 18, 36},
                         set(dataset.ozone_mixing_ratio.chunk_sizes))
        self.assertIsNotNone(dataset.zarr_store.get())

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search(self):
        search_result = list(self.opener.search_data(
            cci_attrs=(dict(ecv='LAKES')))
        )
        self.assertIsNotNone(search_result)
        self.assertEqual(4, len(search_result))
        self.assertIsInstance(search_result[1], DatasetDescriptor)
        self.assertEqual(4, len(search_result[1].dims))
        self.assertEqual(52, len(search_result[1].data_vars))
        self.assertEqual(LAKES_ID, search_result[1].data_id)
        self.assertEqual('1D', search_result[1].time_period)
        self.assertEqual(0.008333333, search_result[1].spatial_res)
        self.assertEqual(DATASET_TYPE, search_result[1].data_type)
        self.assertEqual(('1992-09-15', '2019-12-31'),
                         search_result[1].time_range)
        self.assertEqual('dataset', search_result[0].data_type.alias)
        self.assertEqual('dataset', search_result[1].data_type.alias)
        self.assertEqual('dataset', search_result[2].data_type.alias)


def user_agent(ext: str = "") -> str:
    from esa_climate_toolbox.version import __version__
    from platform import machine, python_version, system

    return "xcube-cci-testing/{version} (Python {python};" \
           " {system} {arch}) {ext}".format(
        version=__version__,
        python=python_version(),
        system=system(),
        arch=machine(),
        ext=ext
    )


class CciCdcDataFrameOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self._opener = CciCdcDataFrameOpener()

    def test_dataset_names(self):
        ds_names = self._opener.dataset_names
        self.assertTrue(len(ds_names) > 10)

    def test_get_dataset_types(self):
        data_types = self._opener.get_data_types()
        self.assertEqual(1, len(data_types))
        self.assertEqual(GEO_DATA_FRAME_TYPE, data_types[0])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self._opener.has_data(GHG_DS_ID))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_data(self):
        descriptor = self._opener.describe_data(GHG_DS_ID)
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, GeoDataFrameDescriptor)
        self.assertEqual(GHG_DS_ID, descriptor.data_id)
        self.assertEqual("WGS84", descriptor.crs)
        self.assertAlmostEqual(-180, descriptor.bbox[0], 1)
        self.assertAlmostEqual(-90, descriptor.bbox[1], 1)
        self.assertAlmostEqual(180, descriptor.bbox[2], 1)
        self.assertAlmostEqual(90, descriptor.bbox[3], 1)
        self.assertEqual("2003-01-08", descriptor.time_range[0])
        self.assertEqual("2012-04-08", descriptor.time_range[1])
        var_list = {
            'solar_zenith_angle', 'sensor_zenith_angle', 'time', 'pressure_levels',
            'pressure_weight', 'xch4_raw', 'xch4', 'h2o_ecmwf', 'xch4_prior',
            'xco2_prior', 'xco2_retrieved', 'xch4_uncertainty', 'xch4_averaging_kernel',
            'ch4_profile_apriori', 'xch4_quality_flag', 'dry_airmass_layer',
            'surface_elevation', 'surface_temperature', 'chi2_ch4', 'chi2_co2',
            'xco2_macc', 'xco2_CT2015', 'xch4_v71', 'geometry'
        }
        self.assertEqual(
            var_list, set(descriptor.feature_schema.properties.keys())
        )

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self._opener.get_open_data_params_schema(GHG_DS_ID).to_dict()
        self.assertIsNotNone(schema)
        self.assertEqual(
            ["variable_names", "time_range", "bbox"],
            list(schema.get("properties").keys())
        )
        var_list = {
            'solar_zenith_angle', 'sensor_zenith_angle', 'pressure_levels',
            'pressure_weight', 'xch4_raw', 'xch4', 'h2o_ecmwf', 'xch4_prior',
            'xco2_prior', 'xco2_retrieved', 'xch4_uncertainty', 'xch4_averaging_kernel',
            'ch4_profile_apriori', 'xch4_quality_flag', 'dry_airmass_layer',
            'surface_elevation', 'surface_temperature', 'chi2_ch4', 'chi2_co2',
            'xco2_macc', 'xco2_CT2015', 'xch4_v71'
        }
        self.assertEqual(
            var_list,
            set(
                schema.get("properties").get("variable_names").get("items").get("enum")
            )
        )


class CciCdcVectorDataCubeOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self._opener = CciCdcVectorDataCubeOpener()

    def test_dataset_names(self):
        ds_names = self._opener.dataset_names
        self.assertTrue(len(ds_names) > 10)

    def test_get_data_types(self):
        data_types = self._opener.get_data_types()
        self.assertEqual(1, len(data_types))
        self.assertEqual(VECTOR_DATA_CUBE_TYPE, data_types[0])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self._opener.has_data(VDC_ID))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_describe_data_sealevel(self):
        descriptor = self._opener.describe_data(VDC_ID)
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, VectorDataCubeDescriptor)
        self.assertEqual(VDC_ID, descriptor.data_id)
        self.assertEqual('vectordatacube', str(descriptor.data_type))
        self.assertEqual(['nbpoints', 'nbmonth'], list(descriptor.dims.keys()))
        self.assertEqual(1867, descriptor.dims['nbpoints'])
        self.assertEqual(216, descriptor.dims['nbmonth'])
        self.assertEqual(4, len(descriptor.data_vars))
        self.assertTrue('distance_to_coast' in descriptor.data_vars)
        self.assertEqual(1, descriptor.data_vars['distance_to_coast'].ndim)
        self.assertEqual(tuple(['nbpoints']),
                         descriptor.data_vars['distance_to_coast'].dims)
        self.assertEqual('float32', descriptor.data_vars['distance_to_coast'].dtype)
        self.assertEqual('WGS84', descriptor.crs)
        self.assertEqual(('2002-01-01', '2019-12-31'), descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self._opener.get_open_data_params_schema(VDC_ID).to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        data = self._opener.open_data(VDC_ID,
                                      variable_names=["distance_to_coast", "sla"])
        self.assertIsNotNone(data)
        self.assertEqual({"distance_to_coast", "sla"}, set(data.data_vars))
        self.assertEqual({"geometry", "time", "time_bnds"}, set(data.coords))
        self.assertEqual({"nbpoints"}, set(data.distance_to_coast.dims))
        self.assertEqual({50}, set(data.distance_to_coast.chunk_sizes))
        self.assertEqual({1867}, set(data.distance_to_coast.shape))
        self.assertEqual({"nbpoints", "nbmonth"}, set(data.sla.dims))
        self.assertEqual({50, 216}, set(data.sla.chunk_sizes))
        self.assertEqual({1867, 216}, set(data.sla.shape))
        self.assertIsNotNone(data.zarr_store.get())


class CciCdcDataStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        odp_params = {'user_agent': user_agent()}
        self.store = CciCdcDataStore(normalize_data=False, **odp_params)

    def test_get_data_store_params_schema(self):
        cci_store_params_schema = \
            CciCdcDataStore.get_data_store_params_schema().to_dict()
        self.assertIsNotNone(cci_store_params_schema)
        self.assertTrue('endpoint_url' in cci_store_params_schema['properties'])
        self.assertTrue(
            'endpoint_description_url' in cci_store_params_schema['properties']
        )
        self.assertTrue('user_agent' in cci_store_params_schema['properties'])

    def test_get_data_types(self):
        self.assertEqual(
            ('dataset', "geodataframe", "vectordatacube"),
            CciCdcDataStore.get_data_types()
        )

    def test_get_data_types_for_data(self):
        data_types_for_data = self.store.get_data_types_for_data(
            AEROSOL_DAY_ID)
        self.assertEqual(('dataset',), data_types_for_data)

        data_types_for_data = self.store.get_data_types_for_data(OZONE_MON_ID)
        self.assertEqual(('dataset',), data_types_for_data)

        with self.assertRaises(DataStoreError) as dse:
            self.store.get_data_types_for_data('nonsense')
        self.assertEqual("Data resource 'nonsense' does not exist in store",
                         f'{dse.exception}')

    def test_get_search_params(self):
        search_schema = self.store.get_search_params_schema().to_dict()
        self.assertIsNotNone(search_schema)
        self.assertTrue('start_date' in search_schema['properties'])
        self.assertTrue('end_date' in search_schema['properties'])
        self.assertTrue('bbox' in search_schema['properties'])
        self.assertTrue('cci_attrs' in search_schema['properties'])
        cci_properties = search_schema['properties']['cci_attrs']['properties']
        self.assertTrue('ecv' in cci_properties)
        self.assertTrue('frequency' in cci_properties)
        self.assertTrue('institute' in cci_properties)
        self.assertTrue('processing_level' in cci_properties)
        self.assertTrue('product_string' in cci_properties)
        self.assertTrue('product_version' in cci_properties)
        self.assertTrue('data_type' in cci_properties)
        self.assertTrue('sensor' in cci_properties)
        self.assertTrue('platform' in cci_properties)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_geodataframe(self):
        geodataframe_search_result = \
            list(self.store.search_data('geodataframe'))
        self.assertIsNotNone(geodataframe_search_result)
        self.assertTrue(len(geodataframe_search_result) > 20)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_vectordatacube(self):
        vectordatacube_search_result = list(self.store.search_data('vectordatacube'))
        self.assertIsNotNone(vectordatacube_search_result)
        self.assertTrue(len(vectordatacube_search_result) > 10)


class CciDataNormalizationTest(unittest.TestCase):

    @skip('Execute to test whether all data sets can be normalized')
    def test_normalization(self):
        store = CciCdcDataStore(normalize_data=True)
        all_data = store.search_data()
        datasets_without_variables = []
        datasets_with_unsupported_frequencies = []
        datasets_that_could_not_be_opened = {}
        good_datasets = []
        for data in all_data:
            if 'climatology' in data.data_id:
                print(f'Cannot read data due to unsupported frequency')
                datasets_with_unsupported_frequencies.append(data.data_id)
                continue
            if not data.data_vars or len(data.data_vars) < 1:
                print(f'Dataset {data.data_id} '
                      f'has not enough variables to open. Will skip.')
                datasets_without_variables.append(data.data_id)
                continue
            print(f'Attempting to open {data.data_id} ...')
            variable_names = []
            for i in range(min(3, len(data.data_vars))):
                variable_names.append(list(data.data_vars.keys())[i])
            start_time = dt.datetime.strptime(data.time_range[0],
                                              '%Y-%m-%d').timestamp()
            end_time = dt.datetime.strptime(data.time_range[1],
                                            '%Y-%m-%d').timestamp()
            center_time = start_time + (end_time - start_time) / 2
            delta = dt.timedelta(days=30)
            range_start = (dt.datetime.fromtimestamp(center_time) - delta).\
                strftime('%Y-%m-%d')
            range_end = (dt.datetime.fromtimestamp(center_time) + delta).\
                strftime('%Y-%m-%d')
            dataset = store.open_data(data_id=data.data_id,
                                      variable_names=variable_names,
                                      time_range=[range_start, range_end]
                                      )
            print(f'Attempting to normalize {data.data_id} ...')
            cube = normalize_dataset(dataset)
            try:
                print(f'Asserting {data.data_id} ...')
                assert_cube(cube)
                good_datasets.append(data.data_id)
            except ValueError as e:
                print(e)
                datasets_that_could_not_be_opened[data.data_id] = e
                continue
        print(f'Datasets with unsupported frequencies '
              f'(#{len(datasets_with_unsupported_frequencies)}): '
              f'{datasets_with_unsupported_frequencies}')
        print(f'Datasets without variables '
              f'(#{len(datasets_without_variables)}): '
              f'{datasets_without_variables}')
        print(f'Datasets that could not be opened '
              f'(#{len(datasets_that_could_not_be_opened)}): '
              f'{datasets_that_could_not_be_opened}')
        print(f'Datasets that were verified '
              f'(#{len(good_datasets)}): {good_datasets}')
