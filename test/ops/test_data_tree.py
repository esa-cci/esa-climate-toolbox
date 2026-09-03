"""Tests for DataTree operations."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import geopandas as gpd
import numpy as np
import xarray as xr
from shapely.geometry import box

from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.ops.data_tree import dataset_to_datatree, dataset_to_lakes_datatree


class DatasetToLakesDatatreeTest(TestCase):

    def setUp(self):
        self.ds = xr.Dataset(
            {'value': (('lat', 'lon'), np.arange(2750).reshape(55, 50))},
            coords={'lat': [r / 10 for r in range(105, 50, -1)],
                    'lon': [r / 10 for r in range(-800, -750, 1)]},
        )

    def test_without_continent_groups(self):
        tree = dataset_to_lakes_datatree(
            self.ds,
            group_into_continents=False
        )

        self.assertEqual(0, len(tree.dataset.data_vars))
        self.assertEqual(4, len(tree.leaves))
        self.assertSetEqual(
            {"Bayano", "Cienaga-De-Ayapel", "Panama", "HYLA00009854"},
            set(tree.keys())
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.2, 9.), lon=slice(-78.9, -78.5)),
            tree['/Bayano'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(8.5, 8.3), lon=slice(-75.2, -75.1)),
            tree['/Cienaga-De-Ayapel'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.3, 9.), lon=slice(-80, -79.7)),
            tree['/Panama'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(10.4, 10.3), lon=slice(-75.2, -75.1)),
            tree['/HYLA00009854'].to_dataset()
        )

    def test_with_continent_groups(self):
        tree = dataset_to_lakes_datatree(
            self.ds,
            group_into_continents=True
        )

        self.assertEqual(0, len(tree.dataset.data_vars))
        self.assertEqual(4, len(tree.leaves))
        self.assertSetEqual(
            {"North America", "South America"},
            set(tree.keys())
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.2, 9.), lon=slice(-78.9, -78.5)),
            tree['/North America/Bayano'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(8.5, 8.3), lon=slice(-75.2, -75.1)),
            tree['/South America/Cienaga-De-Ayapel'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.3, 9.), lon=slice(-80, -79.7)),
            tree['/North America/Panama'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(10.4, 10.3), lon=slice(-75.2, -75.1)),
            tree['/South America/HYLA00009854'].to_dataset()
        )

    def test_with_lake_subsets(self):
        tree = dataset_to_lakes_datatree(
            self.ds,
            lakes=["Bayano", "Cienaga-De-Ayapel"]
        )

        self.assertEqual(0, len(tree.dataset.data_vars))
        self.assertEqual(2, len(tree.leaves))
        self.assertSetEqual(
            {"North America", "South America"},
            set(tree.keys())
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.2, 9.), lon=slice(-78.9, -78.5)),
            tree['/North America/Bayano'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(8.5, 8.3), lon=slice(-75.2, -75.1)),
            tree['/South America/Cienaga-De-Ayapel'].to_dataset()
        )

    def test_with_region_subsets(self):
        tree = dataset_to_lakes_datatree(
            self.ds,
            region=(-80., 5., -77.5, 10.5)
        )

        self.assertEqual(0, len(tree.dataset.data_vars))
        self.assertEqual(1, len(tree.keys()))
        self.assertSetEqual({"North America"}, set(tree.keys()))
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.2, 9.), lon=slice(-78.9, -78.5)),
            tree['/North America/Bayano'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lat=slice(9.3, 9.), lon=slice(-80, -79.7)),
            tree['/North America/Panama'].to_dataset()
        )

class DatasetToDatatreeTest(TestCase):

    def setUp(self):
        self.ds = xr.Dataset(
            {'value': (('lat', 'lon'), np.arange(20).reshape(4, 5))},
            coords={'lat': [3., 2., 1., 0.], 'lon': [0., 1., 2., 3., 4.]},
        )
        self.regions = gpd.GeoDataFrame(
            {
                'region_id': ['west', 'east'],
                'incomplete_region_id': ['west', None],
                'super_id': ['one', 'two'],
                'hyper_id': ['a', 'b'],
                'geometry': [box(0., 0., 1., 3.), box(3., 0., 4., 3.)],
            },
            crs='EPSG:4326',
        )

    def test_from_geodataframe_with_groups(self):
        tree = dataset_to_datatree(
            self.ds,
            'region_id',
            vector_data=self.regions,
            group_columns=['super_id'],
        )

        self.assertEqual(0, len(tree.dataset.data_vars))
        self.assertEqual(2, len(tree.leaves))
        self.assertSetEqual({"one", "two"}, set(tree.keys()))
        self.assertListEqual(["west"], list(tree.get("one").keys()))
        self.assertListEqual(["east"], list(tree.get("two").keys()))
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(0., 1.)), tree['/one/west'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(3., 4.)), tree['/two/east'].to_dataset()
        )

    def test_from_vector_file(self):
        with TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / 'regions.geojson')
            self.regions.to_file(path, driver='GeoJSON')
            tree = dataset_to_datatree(
                self.ds, 'region_id', vector_path=str(path)
            )

            self.assertEqual(0, len(tree.dataset.data_vars))
            self.assertEqual(2, len(tree.leaves))
            self.assertSetEqual({"west", "east"}, set(tree.keys()))
            xr.testing.assert_identical(
                self.ds.sel(lon=slice(0., 1.)), tree['/west'].to_dataset()
            )
            xr.testing.assert_identical(
                self.ds.sel(lon=slice(3., 4.)), tree['/east'].to_dataset()
            )

    def test_requires_exactly_one_region_source(self):
        with self.assertRaisesRegex(ValidationError, 'Exactly one'):
            dataset_to_datatree(self.ds, 'region_id')
        with self.assertRaisesRegex(ValidationError, 'Exactly one'):
            dataset_to_datatree(
                self.ds,
                'region_id',
                vector_data=self.regions,
                vector_path='regions.geojson',
            )

    def test_rejects_duplicate_paths(self):
        regions = self.regions.copy()
        regions['region_id'] = 'same'
        regions['super_id'] = 'same'
        with self.assertRaisesRegex(ValidationError, 'Multiple regions'):
            dataset_to_datatree(
                self.ds,
                'region_id',
                vector_data=regions,
                group_columns=['super_id'],
            )

    def test_reprojects_regions(self):
        regions = self.regions.iloc[:1].to_crs('EPSG:3857')
        tree = dataset_to_datatree(
            self.ds, 'region_id', vector_data=regions
        )
        self.assertIn('/west', tree.groups)

    def test_use_fallback_column(self):
        tree = dataset_to_datatree(
            self.ds, 'incomplete_region_id', vector_data=self.regions, fallback_column='super_id'
        )

        self.assertEqual(2, len(tree.leaves))
        self.assertSetEqual({"west", "two"}, set(tree.keys()))
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(0., 1.)), tree['/west'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(3., 4.)), tree['/two'].to_dataset()
        )

    def test_multiple_group_columns(self):
        tree = dataset_to_datatree(
            self.ds, 'region_id', vector_data=self.regions, group_columns=['super_id', 'hyper_id']
        )

        self.assertEqual(2, len(tree.leaves))
        self.assertSetEqual({"one", "two"}, set(tree.keys()))
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(0., 1.)), tree['/one/a/west'].to_dataset()
        )
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(3., 4.)), tree['/two/b/east'].to_dataset()
        )

    def test_geometry_outside_ds(self):
        shifted_ds = xr.Dataset(
            {'value': (('lat', 'lon'), np.arange(12).reshape(4, 3))},
            coords={'lat': [3., 2., 1., 0.], 'lon': [2., 3., 4.]},
        )

        tree = dataset_to_datatree(
            shifted_ds, 'region_id', vector_data=self.regions
        )

        self.assertEqual(1, len(tree.leaves))
        self.assertSetEqual({"east"}, set(tree.keys()))
        xr.testing.assert_identical(
            shifted_ds.sel(lon=slice(3., 4.)), tree['/east'].to_dataset()
        )

    def test_geometry_only_partly_in_ds(self):
        shifted_ds = xr.Dataset(
            {'value': (('lat', 'lon'), np.arange(10).reshape(2, 5))},
            coords={'lat': [1., 0.], 'lon': [0., 1., 2., 3., 4.]},
        )

        tree = dataset_to_datatree(
            shifted_ds, 'region_id', vector_data=self.regions
        )

        self.assertEqual(2, len(tree.leaves))
        self.assertSetEqual({"west", "east"}, set(tree.keys()))
        xr.testing.assert_identical(
            shifted_ds.sel(lon=slice(0., 1.)), tree['/west'].to_dataset()
        )
        xr.testing.assert_identical(
            shifted_ds.sel(lon=slice(3., 4.)), tree['/east'].to_dataset()
        )

    def test_features_subset(self):
        tree = dataset_to_datatree(
            self.ds, 'region_id', vector_data=self.regions, features=["west"]
        )

        self.assertEqual(1, len(tree.leaves))
        self.assertSetEqual({"west"}, set(tree.keys()))
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(0., 1.)), tree['/west'].to_dataset()
        )

    def test_region_subset(self):
        tree = dataset_to_datatree(
            self.ds, 'region_id', vector_data=self.regions, region=(2., 0., 4., 4.)
        )

        self.assertEqual(1, len(tree.leaves))
        self.assertSetEqual({"east"}, set(tree.keys()))
        xr.testing.assert_identical(
            self.ds.sel(lon=slice(3., 4.)), tree['/east'].to_dataset()
        )
