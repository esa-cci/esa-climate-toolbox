import os
import shutil
import unittest

from unittest import skipIf

from xcube.core.new import new_cube
from xcube.core.store import DataStore

from esa_climate_toolbox.constants import ECT_STORE_ID
from esa_climate_toolbox.constants import ECT_KC_STORE_ID
from esa_climate_toolbox.constants import ECT_ZARR_STORE_ID

from esa_climate_toolbox.core.ds import add_local_store
from esa_climate_toolbox.core.ds import get_store
from esa_climate_toolbox.core.ds import list_datasets
from esa_climate_toolbox.core.ds import list_ecvs
from esa_climate_toolbox.core.ds import list_ecv_datasets
from esa_climate_toolbox.core.ds import list_stores
from esa_climate_toolbox.core.ds import remove_store
from esa_climate_toolbox.core.ds import write_data
from esa_climate_toolbox.core.types import ValidationError


class DsTest(unittest.TestCase):

    def test_list_stores(self):
        provided_stores = list_stores()
        self.assertIn(ECT_STORE_ID, provided_stores)
        self.assertIn(ECT_KC_STORE_ID, provided_stores)
        self.assertIn(ECT_ZARR_STORE_ID, provided_stores)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_list_ecvs(self):
        ecvs = list_ecvs()
        expected_ecvs = [
            'SEAICE', 'ICESHEETS', 'BIOMASS', 'CLOUD', 'FIRE', 'SEALEVEL',
            'WATERVAPOUR', 'LC', 'OZONE', 'LST', 'OC', 'LAKES', 'SNOW', 'SST',
            'GHG', 'SEASTATE', 'AEROSOL', 'SEASURFACESALINITY', 'PERMAFROST',
            'SOILMOISTURE'
        ]
        for expected_ecv in expected_ecvs:
            self.assertIn(expected_ecv, ecvs)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_list_ecv_datasets_fail(self):
        with self.assertRaises(ValueError) as ve:
            list_ecv_datasets('Wind')
        expected = \
            '"Wind" is not an Essential Climate Variable provided by ' \
            'the ESA Climate Toolbox. Please choose one of the following:'
        self.assertEqual(expected, str(ve.exception)[:len(expected)])

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_list_ecv_datasets_lc(self):
        lakes_datasets = list_ecv_datasets('LC')
        expected_zarr_file = (
            'ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-2015-v2.0.7b.zarr',
            ECT_ZARR_STORE_ID
        )
        self.assertIn(expected_zarr_file, lakes_datasets)

    def test_get_store(self):
        store = get_store(store_id=ECT_STORE_ID)
        self.assertIsNotNone(store)
        self.assertIsInstance(store, DataStore)

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_list_datasets(self):
        datasets = list_datasets(store_id=ECT_KC_STORE_ID)
        self.assertTrue(len(datasets) > 1)
        self.assertTrue(
            'ESACCI-LC-L4-PFT-Map-300m-P1Y-1992-2020-v2.0.8-kr1.1'
            in datasets
        )

        datasets = list_datasets(store_id=ECT_ZARR_STORE_ID)
        self.assertTrue(len(datasets) > 1)
        self.assertTrue(
            'ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-2015-v2.0.7b.zarr'
            in datasets
        )


class DsStoresTest(unittest.TestCase):

    def setUp(self) -> None:
        self._root = os.path.join(os.path.dirname(__file__), 'test_data')

    def test_add_local_store_id_taken(self):
        with self.assertRaises(ValueError) as ve:
            add_local_store(root=self._root, store_id=ECT_STORE_ID)
        self.assertEqual(
            f'There is already a store with the id "{ECT_STORE_ID}" '
            f'registered. Please choose another id.',
            str(ve.exception)
        )

    def test_remove_store_essential(self):
        with self.assertRaises(ValueError) as ve:
            remove_store(ECT_STORE_ID, persist=False)
        self.assertEqual(
            f'Cannot remove essential store "{ECT_STORE_ID}".',
            str(ve.exception)
        )

    def test_remove_store_missing(self):
        with self.assertRaises(ValueError) as ve:
            remove_store('vtmcj', persist=False)
        self.assertEqual(
            'No store named "vtmcj" found.',
            str(ve.exception)
        )

    def test_full_workflow_local(self):
        local_store_id = add_local_store(
            root=self._root, persist=False
        )
        self.assertTrue(local_store_id.startswith('local_'))
        self.assertIn(local_store_id, list_stores())
        self.assertNotIn(
            'small/'
            'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc',
            list_datasets(local_store_id)
        )

        other_local_store_id = add_local_store(
            root=self._root, max_depth=2, persist=False
        )

        self.assertTrue(other_local_store_id.startswith('local_'))
        self.assertIn(other_local_store_id, list_stores())
        self.assertIn(
            'small/'
            'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc',
            list_datasets(other_local_store_id)
        )
        self.assertIn(
            'small/'
            'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc',
            list_datasets(other_local_store_id, data_type='dataset')
        )
        self.assertNotIn(
            'small/'
            'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc',
            list_datasets(other_local_store_id, data_type='geodataframe')
        )

        remove_store(local_store_id, persist=False)
        self.assertFalse(local_store_id in list_stores())
        remove_store(other_local_store_id, persist=False)
        self.assertFalse(other_local_store_id in list_stores())

    def test_full_workflow_local_zarr_dataset_paths(self):
        local_zarr_store_id = add_local_store(
            root=self._root, max_depth=2, includes='*.zarr',
            persist=False
        )
        self.assertTrue(local_zarr_store_id.startswith('local_'))
        self.assertIn(local_zarr_store_id, list_stores())
        self.assertEqual([], list_datasets(local_zarr_store_id))
        remove_store(local_zarr_store_id, persist=False)
        self.assertFalse(local_zarr_store_id in list_stores())

    def test_full_workflow_local_nc_dataset_paths(self):
        local_nc_store_id = add_local_store(
            root=self._root, max_depth=2, includes='*.nc',
            persist=False
        )
        self.assertTrue(local_nc_store_id.startswith('local_'))
        self.assertIn(local_nc_store_id, list_stores())
        self.assertIn(
            'small/'
            'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc',
            list_datasets(local_nc_store_id)
        )
        remove_store(local_nc_store_id, persist=False)
        self.assertFalse(local_nc_store_id in list_stores())


class WriteTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cube = new_cube()
        self.path = os.path.join(os.getcwd(), 'test_path')
        self.local_store_id = add_local_store(root=self.path, persist=False)
        os.makedirs(self.path, exist_ok=True)

    def tearDown(self) -> None:
        remove_store(store_id=self.local_store_id)
        shutil.rmtree(self.path)

    def test_write_no_output_store_set(self):
        with self.assertRaises(ValueError) as ve:
            write_data(self.cube)
        self.assertEqual(
            'No default output store set. Must specify target store id.',
            str(ve.exception)
        )

    def test_write_invalid_output_store(self):
        with self.assertRaises(TypeError) as te:
            write_data(self.cube, store_id=ECT_STORE_ID)
        self.assertEqual(
            "value must be an instance of "
            "<class 'xcube.core.store.store.MutableDataStore'>, "
            "was <class 'esa_climate_toolbox.ds.dataaccess.CciCdcDataStore'>",
            str(te.exception)
        )

    def test_write_wrong_format(self):
        with self.assertRaises(ValidationError) as ve:
            write_data(
                self.cube, store_id=self.local_store_id, format_id='geojson'
            )
        self.assertEqual(
            f'Format "geojson" is not supported by data store '
            f'"{self.local_store_id}". Must be one of the following: '
            f'netcdf, zarr, levels, geotiff',
            str(ve.exception)
        )

    def test_write_nc(self):
        data_id = write_data(
            self.cube, store_id=self.local_store_id, format_id='netcdf'
        )
        self.assertTrue(data_id.endswith('.nc'))
        self.assertIn(data_id, list_datasets(self.local_store_id))

    def test_write_zarr(self):
        data_id = write_data(
            self.cube, store_id=self.local_store_id
        )
        self.assertTrue(data_id.endswith('.zarr'))
        self.assertIn(data_id, list_datasets(self.local_store_id))



