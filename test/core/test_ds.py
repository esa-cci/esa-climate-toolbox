import itertools
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import pyproj
import random
import shutil
import unittest
import xarray as xr

from shapely import Point
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
from esa_climate_toolbox.core.ds import list_ecv_datasets_of_titles
from esa_climate_toolbox.core.ds import list_stores
from esa_climate_toolbox.core.ds import open_data
from esa_climate_toolbox.core.ds import remove_store
from esa_climate_toolbox.core.ds import write_data
from esa_climate_toolbox.core.types import ValidationError


def new_vector_data_cube(
    title="Test Vector Data Cube",
    geometries=None,
    num_geometries=20,
    geometry_name="geometry",
    bbox=(-180.0, -90.0, 180.0, 90.0),
    time_name="time",
    time_dtype="datetime64[ns]",
    time_units="seconds since 1970-01-01T00:00:00",
    time_calendar="proleptic_gregorian",
    time_periods=5,
    time_freq="D",
    time_start="2010-01-01T00:00:00",
    use_cftime=False,
    drop_bounds=False,
    variables=None,
    crs="WGS 84",
    crs_name=None,
    time_encoding_dtype="int64",
):
    if isinstance(crs, str):
        crs = pyproj.CRS.from_string(crs)

    if isinstance(crs, pyproj.CRS):
        crs_name = crs_name or "crs"

    if geometries:
        num_geometries = len(geometries)
    else:
        if num_geometries < 0:
            raise ValueError()
        geometries = []
        for _ in range(num_geometries):
            x = random.uniform(bbox[0], bbox[2])
            y = random.uniform(bbox[1], bbox[3])
            geometries.append(Point(x, y))
    np_array = np.asarray(geometries, dtype=object)

    if time_periods < 0:
        raise ValueError()

    if use_cftime and time_dtype is not None:
        raise ValueError('If "use_cftime" is True,' ' "time_dtype" must not be set.')

    geometry_var = xr.DataArray(np_array, dims=geometry_name)

    if use_cftime:
        time_data_p1 = xr.cftime_range(
            start=time_start,
            periods=time_periods + 1,
            freq=time_freq,
            calendar=time_calendar,
        ).values
    else:
        time_data_p1 = pd.date_range(
            start=time_start, periods=time_periods + 1, freq=time_freq
        ).values
        time_data_p1 = time_data_p1.astype(dtype=time_dtype)

    time_delta = time_data_p1[1] - time_data_p1[0]
    time_data = time_data_p1[0:-1] + time_delta // 2
    time_var = xr.DataArray(time_data, dims=time_name)
    time_var.encoding["units"] = time_units
    time_var.encoding["calendar"] = time_calendar
    time_var.encoding["dtype"] = time_encoding_dtype

    coords = {geometry_name: geometry_var, time_name: time_var}
    if not drop_bounds:
        time_bnds_name = f"{time_name}_bnds"

        bnds_dim = "bnds"

        time_bnds_data = np.zeros((time_periods, 2), dtype=time_data_p1.dtype)
        time_bnds_data[:, 0] = time_data_p1[:-1]
        time_bnds_data[:, 1] = time_data_p1[1:]
        time_bnds_var = xr.DataArray(time_bnds_data, dims=(time_name, bnds_dim))
        time_bnds_var.encoding["units"] = time_units
        time_bnds_var.encoding["calendar"] = time_calendar
        time_bnds_var.encoding["dtype"] = time_encoding_dtype

        time_var.attrs["bounds"] = time_bnds_name

        coords.update(
            {
                time_bnds_name: time_bnds_var,
            }
        )

    attrs = dict(
        Conventions="CF-1.7",
        title=title,
        time_coverage_start=str(time_data_p1[0]),
        time_coverage_end=str(time_data_p1[-1]),
    )

    if crs.to_epsg() == 4326:
        attrs.update(
            dict(
                geospatial_lon_min=bbox[0],
                geospatial_lon_max=bbox[2],
                geospatial_lat_min=bbox[1],
                geospatial_lat_max=bbox[3],
                geospatial_units="degree",

            )
        )

    data_vars = {}
    if variables:
        dims = (time_name, geometry_name)
        shape = (time_periods, num_geometries)
        size = time_periods * num_geometries
        for var_name, data in variables.items():
            if isinstance(data, xr.DataArray):
                data_vars[var_name] = data
            elif (
                isinstance(data, int)
                or isinstance(data, float)
                or isinstance(data, bool)
            ):
                data_vars[var_name] = xr.DataArray(np.full(shape, data), dims=dims)
            elif callable(data):
                func = data
                data = np.zeros(shape)
                for index in itertools.product(*map(range, shape)):
                    data[index] = func(*index)
                data_vars[var_name] = xr.DataArray(data, dims=dims)
            elif data is None:
                data_vars[var_name] = xr.DataArray(
                    np.random.uniform(0.0, 1.0, size).reshape(shape), dims=dims
                )
            else:
                data_vars[var_name] = xr.DataArray(data, dims=dims)

    if isinstance(crs, pyproj.CRS):
        for v in data_vars.values():
            v.attrs["grid_mapping"] = crs_name
        data_vars[crs_name] = xr.DataArray(0, attrs=crs.to_cf())

    ds = xr.Dataset(
        # geometry_column=geometry_name, crs=crs,
        data_vars=data_vars,
        coords=coords, attrs=attrs
    )
    ds = ds.xvec.set_geom_indexes(geometry_name, crs=crs)
    return ds


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

    @skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', '1') == '1',
            'ECT_DISABLE_WEB_TESTS = 1')
    def test_list_ecv_datasets_of_titles(self):
        lst_datasets = list_ecv_datasets_of_titles(
            "ESA Land Surface Temperature Climate Change Initiative (LST_cci): "
            "All-weather MicroWave Land Surface Temperature (MW-LST) "
            "global data record (1996-2020), v2.33"
        )
        expected_lst_datasets = [
            ("esacci.LST.day.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.ASC",
             "esa-cci"),
            ("esacci.LST.day.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.DES",
             "esa-cci"),
            ("esacci.LST.mon.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.ASC",
             "esa-cci"),
            ("esacci.LST.mon.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.DES",
             "esa-cci"),
            ("esacci.LST.yr.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.ASC",
             "esa-cci"),
            ("esacci.LST.yr.L3C.LST.multi-sensor.multi-platform.SSMI_SSMIS.v2-33.DES",
             "esa-cci"),
        ]
        self.assertEqual(expected_lst_datasets, lst_datasets)

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
            "was <class 'xcube_cci.dataaccess.CciOdpDataStore'>",
            str(te.exception)
        )

    def test_write_wrong_format(self):
        with self.assertRaises(ValidationError) as ve:
            write_data(
                self.cube, store_id=self.local_store_id, format_id='geojson'
            )
        self.assertIn(
            f'Format "geojson" is not supported by data store '
            f'"{self.local_store_id}".',
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

    ## TODO enable when properly supported by xcube
    # def test_write_geodataframe(self):
        # gdf = gpd.GeoDataFrame(
        #     {
        #         "placename": ["Place A", "Place B"],
        #         "state": ["Active", "Disabled"],
        #         "var_x": [10, 20],
        #         "var_y": [0.5, 2.0]
        #     },
        #     geometry=gpd.points_from_xy([8.0, 8.1], [50.0, 50.1]),
        #     crs="EPSG:4326"
        # )
        # data_id = write_data(
        #     gdf, store_id=self.local_store_id
        # )
        # self.assertIsNotNone(data_id)
        # self.assertTrue(data_id.endswith('.geojson'))
        # self.assertIn(data_id, list_datasets(self.local_store_id))

    def test_write_and_open_vectordatacube(self):
        vdc = new_vector_data_cube(
            variables=dict(
                precipitation=0.5,
                soilmoisture=1.0
            )
        )
        data_id = write_data(
            vdc, store_id=self.local_store_id
        )
        self.assertIsNotNone(data_id)
        self.assertTrue(data_id.endswith('.zarr'))
        self.assertIn(data_id, list_datasets(self.local_store_id))

        vdc_2 = open_data(data_id, data_store_id=self.local_store_id)

        self.assertIsNotNone(vdc_2)
