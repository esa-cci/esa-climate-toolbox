import os
import warnings
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional
from unittest import TestCase, skipIf

import fsspec
import geopandas as gpd
import pandas as pd
import pytest
import xcube
from packaging.version import Version
from xcube.core.store import (
    DataDescriptor,
    DataStoreError,
    GeoDataFrameDescriptor,
    MutableDataStore,
    new_fs_data_store,
)
from xcube.core.store.fs.store import FsDataStore
from xcube.util.temp import new_temp_dir

ROOT_DIR = "esa_climate_toolbox"
DATA_PATH = "testing/data"


def new_geodataframe():
    time_data = pd.date_range(start="2010-01-01T00:00:00", periods=2, freq="D").values
    return gpd.GeoDataFrame(
        {
            "place_name": ["Place A", "Place B"],
            "is_active": [True, False],
            "timestamp": time_data,
            "salinity [‰]": [10, 20],
            "var_y": [0.5, 2.0],
        },
        geometry=gpd.points_from_xy([8.0, 8.1], [50.0, 50.1]),
        crs="EPSG:4326",
    )


# noinspection PyUnresolvedReferences,PyPep8Naming
class FsDataStoresTestMixin(ABC):
    @abstractmethod
    def create_data_store(self) -> FsDataStore:
        pass

    @classmethod
    def prepare_fs(cls, fs: fsspec.AbstractFileSystem, root: str):
        if fs.isdir(root):
            fs.delete(root, recursive=True)

        fs.mkdirs(root)

        # Write a text file into each subdirectory, so
        # we also test that store.get_data_ids() scans
        # recursively.
        dir_path = root
        for subdir_name in DATA_PATH.split("/"):
            dir_path += "/" + subdir_name
            fs.mkdir(dir_path)
            file_path = dir_path + "/README.md"
            with fs.open(file_path, "w") as fp:
                fp.write("\n")

    def test_geodataframe_kml(self):
        data_store = self.create_data_store()
        self._assert_geodataframe_supported(
            data_store,
            filename_ext=".kml",
            requested_dtype_alias="geodataframe",
            expected_dtype_aliases={"geodataframe"},
            expected_return_type=gpd.GeoDataFrame,
            expected_descriptor_type=GeoDataFrameDescriptor,
            assert_data_ok=self._assert_geodataframe_ok,
        )

    def _assert_geodataframe_ok(self, gdf: gpd.GeoDataFrame):
        self.assertIn("place_name", gdf.columns)
        self.assertIn("is_active", gdf.columns)
        self.assertIn("salinity [‰]", gdf.columns)
        self.assertIn("var_y", gdf.columns)
        self.assertIn("geometry", gdf.columns)
        self.assertIn("timestamp", gdf.columns)
        self.assertEqual("object", str(gdf.place_name.dtype))
        self.assertEqual("bool", str(gdf.is_active.dtype))
        self.assertEqual("int32", gdf["salinity [‰]"].dtype)
        self.assertEqual("float64", str(gdf.var_y.dtype))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(gdf.timestamp))

    def _assert_geodataframe_supported(
        self,
        data_store: FsDataStore,
        filename_ext: str,
        requested_dtype_alias: Optional[str],
        expected_dtype_aliases: set[str],
        expected_return_type: type[gpd.GeoDataFrame],
        expected_descriptor_type: Optional[type[GeoDataFrameDescriptor]] = None,
        opener_id: str = None,
        write_params: Optional[dict[str, Any]] = None,
        open_params: Optional[dict[str, Any]] = None,
        assert_data_ok: Optional[Callable[[Any], Any]] = None,
        assert_warnings: bool = False,
        warning_msg: str = None,
    ):
        """Call all DataStore operations to ensure data of type
        gpd.GeoDataFrame is supported by *data_store*.

        Args:
            data_store: The filesystem data store instance.
            filename_ext: Filename extension that identifies
                a supported dataset format.
            expected_data_type_alias: The expected data type alias.
            expected_return_type: The expected data type.
            expected_descriptor_type: The expected data descriptor type.
            opener_id: Optional opener identifier
            write_params: Optional write parameters
            open_params: Optional open parameters
            assert_data_ok: Optional function to assert read data is ok
            assert_warnings: Optional boolean if test may check for warnings
            warning_msg: Optional warning message to be checked if
                assert_warnings is True
        """

        data_id = f"{DATA_PATH}/ds{filename_ext}"

        write_params = write_params or {}
        open_params = open_params or {}

        self.assertIsInstance(data_store, MutableDataStore)

        self.assertEqual(
            {"dataset", "mldataset", "geodataframe"}, set(data_store.get_data_types())
        )

        with pytest.raises(
            DataStoreError, match=f'Data resource "{data_id}" does not exist in store'
        ):
            data_store.get_data_types_for_data(data_id)
        self.assertEqual(False, data_store.has_data(data_id))
        self.assertNotIn(data_id, set(data_store.get_data_ids()))
        self.assertNotIn(data_id, data_store.list_data_ids())

        data = new_geodataframe()
        written_data_id = data_store.write_data(data, data_id, **write_params)
        self.assertEqual(data_id, written_data_id)

        self.assertEqual(
            expected_dtype_aliases, set(data_store.get_data_types_for_data(data_id))
        )
        self.assertEqual(True, data_store.has_data(data_id))
        self.assertIn(data_id, set(data_store.get_data_ids()))
        self.assertIn(data_id, data_store.list_data_ids())

        if expected_descriptor_type is not None:
            data_descriptors = list(
                data_store.search_data(data_type=expected_return_type)
            )
            self.assertEqual(1, len(data_descriptors))
            self.assertIsInstance(data_descriptors[0], DataDescriptor)
            self.assertIsInstance(data_descriptors[0], expected_descriptor_type)

        if assert_warnings:
            with warnings.catch_warnings(record=True) as w:
                data = data_store.open_data(
                    data_id,
                    opener_id=opener_id,
                    data_type=requested_dtype_alias,
                    **open_params,
                )
            # if "s3" data store is tested, warnings from other
            # libraries like botocore occur
            if data_store.protocol != "s3":
                self.assertEqual(1, len(w))
            self.assertEqual(w[0].category, UserWarning)
            self.assertEqual(warning_msg, w[0].message.args[0])
        else:
            data = data_store.open_data(
                data_id,
                opener_id=opener_id,
                data_type=requested_dtype_alias,
                **open_params,
            )
        self.assertIsInstance(data, expected_return_type)
        if assert_data_ok is not None:
            assert_data_ok(data)

        try:
            data_store.delete_data(data_id)
        except PermissionError as e:  # May occur on win32 due to fsspec
            warnings.warn(f"{e}")
            return
        with pytest.raises(
            DataStoreError,
            match=f'Data resource "{data_id}" does not exist in store',
        ):
            data_store.get_data_types_for_data(data_id)
        self.assertEqual(False, data_store.has_data(data_id))
        self.assertNotIn(data_id, set(data_store.get_data_ids()))
        self.assertNotIn(data_id, data_store.list_data_ids())


@skipIf(
    Version(xcube.__version__) < Version("1.12.0"), "higher xcube version is required"
)
class FileFsDataStoresTest(FsDataStoresTestMixin, TestCase):
    def create_data_store(self) -> FsDataStore:
        root = os.path.join(new_temp_dir(prefix="xcube"), ROOT_DIR)
        self.prepare_fs(fsspec.filesystem("file"), root)
        return new_fs_data_store("file", root=root, max_depth=3)


@skipIf(
    Version(xcube.__version__) < Version("1.12.0"), "higher xcube version is required"
)
class MemoryFsDataStoresTest(FsDataStoresTestMixin, TestCase):
    def create_data_store(self) -> FsDataStore:
        root = ROOT_DIR
        self.prepare_fs(fsspec.filesystem("memory"), root)
        return new_fs_data_store("memory", root=root, max_depth=3)
