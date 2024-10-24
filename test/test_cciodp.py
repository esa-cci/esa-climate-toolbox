import numpy as np
import os
import pandas as pd
import unittest
from unittest import skip, skipIf

from xcube_cci.ccicdc import get_res
from xcube_cci.ccicdc import CciCdc
from xcube_cci.ccicdc import find_datetime_format
from xcube_cci.constants import OPENSEARCH_CEDA_URL

AEROSOL_ID = 'esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ORAC.04-01-.r1'
CLOUD_ID = 'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1'
OC_DAY_ID = 'esacci.OC.day.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.' \
           '6-0.geographic'
OC_DAY_SIN_ID = 'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.' \
                'MERGED.6-0.sinusoidal'
OC_DAY_31_ID = 'esacci.OC.day.L3S.CHLOR_A.multi-sensor.multi-platform.' \
               'MERGED.3-1.geographic'
OC_MON_ID = \
    'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.6-0.sinusoidal'
OC_MON_31_ID = 'esacci.OC.mon.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.' \
               '3-1.geographic'
OC_5DAYS_GEO_ID = 'esacci.OC.5-days.L3S.K_490.multi-sensor.multi-platform.' \
              'MERGED.6-0.geographic'
OC_5DAYS_SIN_ID = 'esacci.OC.5-days.L3S.K_490.multi-sensor.multi-platform.' \
                  'MERGED.6-0.sinusoidal'
OC_5DAYS_CHLOR_SIN_ID = 'esacci.OC.5-days.L3S.CHLOR_A.multi-sensor.' \
                        'multi-platform.MERGED.6-0.geographic'
OC_5DAYS_CHLOR_SIN_31_ID = 'esacci.OC.5-days.L3S.CHLOR_A.multi-sensor.' \
                           'multi-platform.MERGED.3-1.geographic'
OC_8DAYS_CHLOR_31_ID = 'esacci.OC.8-days.L3S.CHLOR_A.multi-sensor.' \
                       'multi-platform.MERGED.3-1.geographic'
OZONE_ID = 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'
SEAICE_ID = 'esacci.SEAICE.mon.L3C.SITHICK.SIRAL.CryoSat-2.NH25KMEASE2.2-0.r1'
SST_ID = 'esacci.SST.satellite-orbit-frequency.L3U.SSTskin.AVHRR-3.NOAA-19.' \
         'AVHRR19_G.2-1.r1'


class CciCdcTest(unittest.TestCase):

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_data_chunk(self):
        cci_cdc = CciCdc()
        ds_id = cci_cdc.get_dataset_id(OZONE_ID)
        request = dict(parentIdentifier=ds_id,
                       startDate='1997-05-01T00:00:00',
                       endDate='1997-05-02T00:00:00',
                       varNames=['surface_pressure'],
                       drsId=OZONE_ID
                       )
        dim_indexes = (slice(None, None), slice(0, 179), slice(0, 359))
        data = cci_cdc.get_data_chunk(request, dim_indexes)
        self.assertIsNotNone(data)
        data_array = np.frombuffer(data, dtype=np.float32)
        self.assertEqual(64261, len(data_array))
        self.assertAlmostEqual(1024.4185, data_array[-1], 4)

        # check whether data type has been converted to
        request = dict(parentIdentifier=ds_id,
                       startDate='1997-05-01T00:00:00',
                       endDate='1997-05-02T00:00:00',
                       varNames=['layers'],
                       drsId=OZONE_ID)
        dim_indexes = (slice(None, None), slice(0, 179), slice(0, 359))
        data = cci_cdc.get_data_chunk(request, dim_indexes)
        self.assertIsNotNone(data)
        data_array = np.frombuffer(data, dtype=np.int64)
        self.assertEqual(16, len(data_array))
        self.assertAlmostEqual(15, data_array[-2])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_dataset_names(self):
        cci_cdc = CciCdc()
        dataset_names = cci_cdc.dataset_names
        self.assertIsNotNone(dataset_names)
        list(dataset_names)
        self.assertTrue(len(dataset_names) > 250)
        self.assertTrue(AEROSOL_ID in dataset_names)
        self.assertTrue(OC_MON_ID in dataset_names)
        self.assertTrue(SST_ID in dataset_names)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_var_and_coord_names(self):
        cci_cdc = CciCdc()
        var_names, coord_names = cci_cdc.var_and_coord_names(OC_5DAYS_GEO_ID)
        self.assertIsNotNone(var_names)
        self.assertEqual(
            ['MERIS_nobs_sum', 'MODISA_nobs_sum', 'OLCI-A_nobs_sum',
             'OLCI-B_nobs_sum', 'SeaWiFS_nobs_sum', 'VIIRS_nobs_sum', 'crs',
             'kd_490', 'kd_490_bias', 'kd_490_rmsd', 'total_nobs_sum'],
            var_names
        )
        self.assertIsNotNone(coord_names)
        self.assertEqual(['lat', 'lon', 'time'], coord_names)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_dataset_info(self):
        cci_cdc = CciCdc()
        dataset_info = cci_cdc.get_dataset_info(CLOUD_ID)
        self.assertIsNotNone(dataset_info)
        self.assertTrue('y_res' in dataset_info)
        self.assertEqual(0.5, dataset_info['y_res'])
        self.assertTrue('x_res' in dataset_info)
        self.assertEqual(0.5, dataset_info['x_res'])
        self.assertTrue('bbox' in dataset_info)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), dataset_info['bbox'])
        self.assertTrue('var_names' in dataset_info)
        self.assertEqual([
            'nobs', 'nobs_day', 'nobs_clear_day', 'nobs_cloudy_day',
            'nobs_clear_night', 'nobs_cloudy_night', 'nobs_clear_twl',
            'nobs_cloudy_twl', 'nobs_cloudy', 'nretr_cloudy',
            'nretr_cloudy_liq', 'nretr_cloudy_ice', 'nretr_cloudy_day',
            'nretr_cloudy_day_liq', 'nretr_cloudy_day_ice', 'nretr_cloudy_low',
            'nretr_cloudy_mid', 'nretr_cloudy_high', 'cfc', 'cfc_std',
            'cfc_prop_unc', 'cfc_corr_unc', 'cfc_unc', 'cfc_low', 'cfc_mid',
            'cfc_high', 'cfc_day', 'cfc_night', 'cfc_twl', 'ctt', 'ctt_std',
            'ctt_prop_unc', 'ctt_corr_unc', 'ctt_unc', 'ctt_corrected',
            'ctt_corrected_std', 'ctt_corrected_prop_unc',
            'ctt_corrected_corr_unc', 'ctt_corrected_unc', 'stemp', 'stemp_std',
            'stemp_prop_unc', 'stemp_corr_unc', 'stemp_unc', 'cth', 'cth_std',
            'cth_prop_unc', 'cth_corr_unc', 'cth_unc', 'cth_corrected',
            'cth_corrected_std', 'cth_corrected_prop_unc',
            'cth_corrected_corr_unc', 'cth_corrected_unc', 'ctp', 'ctp_std',
            'ctp_prop_unc', 'ctp_corr_unc', 'ctp_unc', 'ctp_log',
            'ctp_corrected', 'ctp_corrected_std', 'ctp_corrected_prop_unc',
            'ctp_corrected_corr_unc', 'ctp_corrected_unc', 'cph', 'cph_std',
            'cph_day', 'cph_day_std', 'cer', 'cer_std', 'cer_prop_unc',
            'cer_corr_unc', 'cer_unc', 'cot', 'cot_log', 'cot_std',
            'cot_prop_unc', 'cot_corr_unc', 'cot_unc', 'cee', 'cee_std',
            'cee_prop_unc', 'cee_corr_unc', 'cee_unc', 'cla_vis006',
            'cla_vis006_std', 'cla_vis006_prop_unc', 'cla_vis006_corr_unc',
            'cla_vis006_unc', 'cla_vis006_liq', 'cla_vis006_liq_std',
            'cla_vis006_liq_unc', 'cla_vis006_ice', 'cla_vis006_ice_std',
            'cla_vis006_ice_unc', 'cla_vis008', 'cla_vis008_std',
            'cla_vis008_prop_unc', 'cla_vis008_corr_unc', 'cla_vis008_unc',
            'cla_vis008_liq', 'cla_vis008_liq_std', 'cla_vis008_liq_unc',
            'cla_vis008_ice', 'cla_vis008_ice_std', 'cla_vis008_ice_unc', 'lwp',
            'lwp_std', 'lwp_prop_unc', 'lwp_corr_unc', 'lwp_unc', 'lwp_allsky',
            'iwp', 'iwp_std', 'iwp_prop_unc', 'iwp_corr_unc', 'iwp_unc',
            'iwp_allsky', 'cer_liq', 'cer_liq_std', 'cer_liq_prop_unc',
            'cer_liq_corr_unc', 'cer_liq_unc', 'cer_ice', 'cer_ice_std',
            'cer_ice_prop_unc', 'cer_ice_corr_unc', 'cer_ice_unc', 'cot_liq',
            'cot_liq_std', 'cot_liq_prop_unc', 'cot_liq_corr_unc',
            'cot_liq_unc', 'cot_ice', 'cot_ice_std', 'cot_ice_prop_unc',
            'cot_ice_corr_unc', 'cot_ice_unc', 'hist2d_cot_ctp', 'hist1d_cot',
            'hist1d_ctp', 'hist1d_ctt', 'hist1d_cer', 'hist1d_cwp',
            'hist1d_cla_vis006', 'hist1d_cla_vis008'],
            dataset_info['var_names']
        )
        self.assertTrue('temporal_coverage_start' in dataset_info)
        self.assertEqual('2000-02-01T00:00:00',
                         dataset_info['temporal_coverage_start'])
        self.assertTrue('temporal_coverage_end' in dataset_info)
        self.assertEqual('2014-12-31T23:59:59',
                         dataset_info['temporal_coverage_end'])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_fetch_data_source_list_json(self):
        cci_cdc = CciCdc()

        async def fetch_data_source_list_json(session):
            return await cci_cdc._fetch_data_source_list_json(
                session, OPENSEARCH_CEDA_URL, {
                    'parentIdentifier': 'cci',
                    'drsId': OC_5DAYS_SIN_ID}
            )

        data_source_list = cci_cdc._run_with_session(
            fetch_data_source_list_json
        )
        self.assertIsNotNone(data_source_list)
        self.assertEqual(1, len(data_source_list))
        self.assertTrue(
            '3bdb21a4cd004e5f8cc148fea5f1d4e3' in data_source_list
        )
        self.assertTrue(len(
            data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3'].items()
        ) >= 6)
        self.assertEqual(
            '3bdb21a4cd004e5f8cc148fea5f1d4e3',
            data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3'].get(
                'uuid', ''
            )
        )
        self.assertEqual(
            'ESA Ocean Colour Climate Change Initiative (Ocean_Colour_cci): '
            'Global attenuation coefficient for downwelling irradiance (Kd490) '
            'gridded on a sinusoidal projection at 4km resolution, Version 6.0',
            data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3'].get(
                'title', ''
            )
        )
        self.assertTrue(
            'variables' in data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3']
        )
        self.assertTrue(
            'odd_url' in data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3']
        )
        self.assertTrue(
            data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3'].
                get('metadata_url', '').endswith(
                '3bdb21a4cd004e5f8cc148fea5f1d4e3.xml'
            )
        )
        self.assertEqual(
            'https://catalogue.ceda.ac.uk/uuid/'
            '3bdb21a4cd004e5f8cc148fea5f1d4e3',
            data_source_list['3bdb21a4cd004e5f8cc148fea5f1d4e3'].get(
                'catalog_url', ''
            )
        )

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_datasets_metadata(self):
        cci_cdc = CciCdc()
        datasets = [OC_DAY_ID, OC_5DAYS_CHLOR_SIN_ID, SEAICE_ID]
        datasets_metadata = cci_cdc.get_datasets_metadata(datasets)
        self.assertIsNotNone(datasets_metadata)
        self.assertEqual(3, len(datasets_metadata))
        self.assertTrue('variables' in datasets_metadata[0])
        self.assertTrue('variable_infos' in datasets_metadata[2])
        self.assertTrue('freeboard' in datasets_metadata[2].
                        get('variable_infos'))
        self.assertEqual(
            {'coordinates': 'time lon lat',
             'grid_mapping': 'Lambert_Azimuthal_Grid',
             'long_name': 'elevation of retracked point above instantaneous '
                          'sea surface height (with snow range corrections)',
             'standard_name': 'sea_ice_freeboard',
             'units': 'm',
             'chunk_sizes': [1, 432, 432],
             'file_chunk_sizes': [1, 432, 432],
             'data_type': 'float32',
             'orig_data_type': 'float32',
             'dimensions': ['time', 'yc', 'xc'],
             'file_dimensions': ['time', 'yc', 'xc'],
             'fill_value': np.nan,
             'size': 8957952,
             'shape': [48, 432, 432]},
            datasets_metadata[2].get('variable_infos').get('freeboard'))
        self.assertEqual('uint16', datasets_metadata[2].get('variable_infos').
                         get('status_flag').get('data_type'))

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while old archive is set up')
    def test_get_drs_metadata(self):
        cci_cdc = CciCdc()
        fid = '12d6f4bdabe144d7836b0807e65aa0e2'
        metadata = {}

        async def set_drs_metadata(session):
            return await cci_cdc._set_drs_metadata(session, fid, metadata)

        cci_cdc._run_with_session(set_drs_metadata)
        variables_dict = metadata['variables']
        drs_uuids = metadata['uuids']
        self.assertIsNotNone(variables_dict)
        self.assertEqual(4, len(variables_dict.items()))
        self.assertTrue(OC_DAY_31_ID in variables_dict)
        self.assertEqual(12, len(variables_dict[OC_DAY_31_ID]))
        variable_names = [
            variable['name'] for variable in variables_dict[OC_DAY_31_ID]
        ]
        self.assertEqual(
            ['chlor_a_log10_bias', 'chlor_a', 'MERIS_nobs', 'MODISA_nobs',
             'SeaWiFS_nobs', 'VIIRS_nobs', 'total_nobs', 'chlor_a_log10_rmsd',
             'lat', 'lon', 'time', 'crs'],
            variable_names)
        self.assertTrue(OC_5DAYS_CHLOR_SIN_31_ID in variables_dict)
        variable_names = [variable['name'] for variable
                          in variables_dict[OC_5DAYS_CHLOR_SIN_31_ID]]
        self.assertEqual(
            ['chlor_a_log10_bias', 'chlor_a', 'MERIS_nobs_sum',
             'MODISA_nobs_sum', 'SeaWiFS_nobs_sum', 'VIIRS_nobs_sum',
             'total_nobs_sum', 'chlor_a_log10_rmsd', 'lat', 'lon', 'time',
             'crs'], variable_names)
        self.assertEqual(4, len(drs_uuids.items()))
        self.assertTrue(OC_DAY_31_ID in drs_uuids)
        self.assertEqual('f13668e057c736410676fcf51983ca9d018108c3',
                         drs_uuids[OC_DAY_31_ID])
        self.assertTrue(OC_5DAYS_CHLOR_SIN_31_ID in drs_uuids)
        self.assertEqual('d43b5fa4c4e0d7edc89f794798aa07824671105b',
                         drs_uuids[OC_5DAYS_CHLOR_SIN_31_ID])
        self.assertTrue(OC_8DAYS_CHLOR_31_ID in drs_uuids)
        self.assertEqual('5082ae00d82255c2389a74593b71ebe686fc1314',
                         drs_uuids[OC_8DAYS_CHLOR_31_ID])
        self.assertTrue(OC_MON_31_ID in drs_uuids)
        self.assertEqual('e7f59d6547d610fdd04ae05cca77ddfee15e3a5a',
                         drs_uuids[OC_MON_31_ID])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_opendap_dataset(self):
        opendap_url = 'http://data.cci.ceda.ac.uk/thredds/dodsC/esacci/' \
                      'aerosol/data/AATSR_SU/L3/v4.21/DAILY/2002/07/' \
                      '20020724-ESACCI-L3C_AEROSOL-AER_PRODUCTS-AATSR_' \
                      'ENVISAT-SU_DAILY-v4.21.nc'
        cci_cdc = CciCdc()
        dataset = cci_cdc.get_opendap_dataset(opendap_url)
        self.assertIsNotNone(dataset)
        self.assertEqual(53, len(list(dataset.keys())))
        self.assertTrue('AOD550_mean' in dataset.keys())
        self.assertEqual('atmosphere_optical_thickness_due_to_ambient_aerosol',
                         dataset['AOD550_mean'].attributes['standard_name'])
        self.assertEqual(('latitude', 'longitude'),
                         dataset['AOD550_mean'].dimensions)
        self.assertEqual(64800, dataset['AOD550_mean'].size)

    def test_get_res(self):
        nc_attrs = dict(geospatial_lat_resolution=24.2,
                        geospatial_lon_resolution=30.1)
        self.assertEqual(24.2, get_res(nc_attrs, 'lat'))
        self.assertEqual(30.1, get_res(nc_attrs, 'lon'))

        nc_attrs = dict(resolution=5.0)
        self.assertEqual(5.0, get_res(nc_attrs, 'lat'))
        self.assertEqual(5.0, get_res(nc_attrs, 'lon'))

        nc_attrs = dict(resolution='12x34 degree')
        self.assertEqual(12.0, get_res(nc_attrs, 'lat'))
        self.assertEqual(34.0, get_res(nc_attrs, 'lon'))

        nc_attrs = dict(spatial_resolution='926.62543305 m')
        self.assertEqual(926.62543305, get_res(nc_attrs, 'lat'))
        self.assertEqual(926.62543305, get_res(nc_attrs, 'lon'))

        nc_attrs = dict(spatial_resolution='60km x 30km at nadir '
                                           '(along-track x across-track)')
        self.assertEqual(60.0, get_res(nc_attrs, 'lat'))
        self.assertEqual(30.0, get_res(nc_attrs, 'lon'))

    def test_find_datetime_format(self):
        time_format, start, end, timedelta = \
            find_datetime_format('fetgzrs2015ydhfbgv')
        self.assertEqual('%Y', time_format)
        self.assertEqual(7, start)
        self.assertEqual(11, end)
        self.assertEqual(1, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(0, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(-1, timedelta.seconds)

        time_format, start, end, timedelta = \
            find_datetime_format('fetz23gxgs20150213ydh391fbgv')
        self.assertEqual('%Y%m%d', time_format)
        self.assertEqual(10, start)
        self.assertEqual(18, end)
        self.assertEqual(0, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(1, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(-1, timedelta.seconds)

        time_format, start, end, timedelta = \
            find_datetime_format('f23gxgs19961130191846y391fbgv')
        self.assertEqual('%Y%m%d%H%M%S', time_format)
        self.assertEqual(7, start)
        self.assertEqual(21, end)
        self.assertEqual(0, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(0, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(0, timedelta.seconds)

        time_format, start, end, timedelta = \
            find_datetime_format('f23gxgdrtgys1983-11-30y391fbgv')
        self.assertEqual('%Y-%m-%d', time_format)
        self.assertEqual(12, start)
        self.assertEqual(22, end)
        self.assertEqual(0, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(1, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(-1, timedelta.seconds)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_variable_data(self):
        cci_cdc = CciCdc()
        dimension_data = cci_cdc.get_variable_data(
            AEROSOL_ID,
            {'latitude': 180,
             'longitude': 360,
             'view': 2,
             'aerosol_type': 10},
            '2002-08-01T00:00:00',
            '2002-08-02T00:00:00')
        self.assertIsNotNone(dimension_data)
        self.assertEqual(dimension_data['latitude']['size'], 180)
        self.assertEqual(dimension_data['latitude']['chunkSize'], 180)
        self.assertEqual(dimension_data['latitude']['data'][0], -89.5)
        self.assertEqual(dimension_data['latitude']['data'][-1], 89.5)
        self.assertEqual(dimension_data['longitude']['size'], 360)
        self.assertEqual(dimension_data['longitude']['chunkSize'], 360)
        self.assertEqual(dimension_data['longitude']['data'][0], -179.5)
        self.assertEqual(dimension_data['longitude']['data'][-1], 179.5)
        self.assertEqual(dimension_data['view']['size'], 2)
        self.assertEqual(dimension_data['view']['chunkSize'], 2)
        self.assertEqual(dimension_data['view']['data'][0], 0)
        self.assertEqual(dimension_data['view']['data'][-1], 1)
        self.assertEqual(dimension_data['aerosol_type']['size'], 10)
        self.assertEqual(dimension_data['aerosol_type']['chunkSize'], 10)
        self.assertEqual(dimension_data['aerosol_type']['data'][0], 0)
        self.assertEqual(dimension_data['aerosol_type']['data'][-1], 9)

        dimension_data = cci_cdc.get_variable_data(
            OC_DAY_SIN_ID, {'lat': 23761676, 'lon': 23761676}
        )
        self.assertIsNotNone(dimension_data)
        self.assertEqual(dimension_data['lat']['size'], 23761676)
        self.assertEqual(dimension_data['lat']['chunkSize'], 72900)
        self.assertEqual(len(dimension_data['lat']['data']), 0)
        self.assertEqual(dimension_data['lon']['size'], 23761676)
        self.assertEqual(dimension_data['lon']['chunkSize'], 72900)
        self.assertEqual(len(dimension_data['lon']['data']), 0)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_ecv(self):
        cci_cdc = CciCdc()
        aerosol_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                ecv='AEROSOL'
            )
        )
        self.assertTrue(len(aerosol_sources) > 13)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_frequency(self):
        cci_cdc = CciCdc()
        five_day_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                frequency='5 days'
            )
        )
        self.assertTrue(len(five_day_sources) > 10)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_processing_level(self):
        cci_cdc = CciCdc()
        l2p_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                processing_level='L2P'
            )
        )
        self.assertTrue(len(l2p_sources) > 10)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_product_string(self):
        cci_cdc = CciCdc()
        avhrr19g_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                product_string='AVHRR19_G'
            )
        )
        self.assertTrue(len(avhrr19g_sources) > 3)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_product_version(self):
        cci_cdc = CciCdc(data_type="geodataframe")
        v238_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                product_version='v2.3.8'
            )
        )
        self.assertTrue(len(v238_sources) > 2)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_data_type(self):
        cci_cdc = CciCdc()
        siconc_sources = cci_cdc.search(
            start_date='2007-05-01',
            end_date='2009-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                data_type='SICONC'
            )
        )
        self.assertTrue(len(siconc_sources) > 3)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_search_sensor(self):
        cci_cdc = CciCdc(data_type="geodataframe")
        sciamachy_sources = cci_cdc.search(
            start_date='1990-05-01',
            end_date='2021-08-01',
            bbox=(-20, 30, 20, 50),
            cci_attrs=dict(
                sensor='SCIAMACHY'
            )
        )
        self.assertTrue(len(sciamachy_sources) > 2)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_get_time_ranges_from_data(self):
        cci_cdc = CciCdc()
        first_time_ranges = cci_cdc.get_time_ranges_from_data(
            dataset_name='esacci.OC.5-days.L3S.RRS.multi-sensor.multi-platform.'
            'MERGED.6-0.geographic',
            start_time='1997-09-03T00:00:00',
            end_time='1997-09-10T00:00:00'
        )
        self.assertEqual([(pd.Timestamp('1997-09-03 00:00:00'),
                          pd.Timestamp('1997-09-07 23:59:00')),
                          (pd.Timestamp('1997-09-08 00:00:00'),
                           pd.Timestamp('1997-09-12 23:59:00'))],
                         first_time_ranges)
        self.assertIsNotNone(first_time_ranges)
        second_time_ranges = cci_cdc.get_time_ranges_from_data(
            dataset_name='esacci.OC.5-days.L3S.RRS.multi-sensor.multi-platform.'
            'MERGED.6-0.geographic',
            start_time='1997-09-10T00:00:00',
            end_time='1997-09-30T00:00:00'
        )
        self.assertIsNotNone(second_time_ranges)
        self.assertEqual([(pd.Timestamp('1997-09-08 00:00:00'),
                          pd.Timestamp('1997-09-12 23:59:00')),
                          (pd.Timestamp('1997-09-13 00:00:00'),
                           pd.Timestamp('1997-09-17 23:59:00')),
                          (pd.Timestamp('1997-09-18 00:00:00'),
                           pd.Timestamp('1997-09-22 23:59:00')),
                          (pd.Timestamp('1997-09-23 00:00:00'),
                           pd.Timestamp('1997-09-27 23:59:00')),
                          (pd.Timestamp('1997-09-28 00:00:00'),
                           pd.Timestamp('1997-10-02 23:59:00'))],
                         second_time_ranges)
