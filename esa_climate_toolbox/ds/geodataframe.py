# Copyright (c) 2018-2025 by xcube team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import geopandas as gpd
import pandas as pd
import simplekml

from xcube.util.assertions import assert_instance
from xcube.util.fspath import is_local_fs
from xcube.util.temp import new_temp_file

from xcube.core.store import DataStoreError
from xcube.core.store.fs.impl.geodataframe import GeoDataFrameFsDataAccessor


class GeoDataFrameKmlFsDataAccessor(GeoDataFrameFsDataAccessor):
    """Extension name: 'geodataframe:kml:<protocol>'."""

    @classmethod
    def get_format_id(cls) -> str:
        return "kml"

    @classmethod
    def get_driver_name(cls) -> str:
        return "KML"

    def open_data(self, data_id: str, **open_params) -> gpd.GeoDataFrame:
        gdf = super().open_data(data_id, **open_params)
        kml_nan_columns = [
            "Name", "description", "timestamp", "begin", "end", "altitudeMode",
            "drawOrder", "icon"
        ]
        kml_number_columns = {
            "tessellate": -1,
            "extrude": 0,
            "visibility": -1,
        }
        for col in gdf.columns:
            if ((col in kml_nan_columns and pd.isna(gdf[col]).all()) or
                (col in kml_number_columns.keys() and
                 len(gdf[col].unique()) == 1 and
                 gdf[col].unique()[0] == kml_number_columns[col])):
                del gdf[col]
                continue
            if col not in ["geometry"]:
                if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                    continue
                try:
                    gdf[col] = pd.to_numeric(gdf[col])
                except ValueError:
                    if gdf[col].isin(["True", "False"]).all():
                        gdf[col] = gdf[col].map({"True": True, "False": False})
        return gdf

    def write_data(self, data: gpd.GeoDataFrame, data_id: str, **write_params) -> str:
        assert_instance(data, (gpd.GeoDataFrame, pd.DataFrame), "data")
        fs, root, write_params = self.load_fs(write_params)
        is_local = is_local_fs(fs)
        replace = write_params.pop("replace", False)
        if is_local:
            file_path = data_id
            if not replace and fs.exists(file_path):
                raise DataStoreError(f"Data '{data_id}' already exists.")
        else:
            _, file_path = new_temp_file()

        kml = simplekml.Kml()
        kml_schema = kml.newschema(name="kmlschema")
        append_cols = {}

        for _, row in data.iterrows():
            geom = row.geometry

            if geom.geom_type == "Point":
                entry = kml.newpoint(coords=[(geom.x, geom.y)])
            elif geom.geom_type == "LineString":
                entry = kml.newlinestring(coords=list(geom.coords))
            elif geom.geom_type == "Polygon":
                entry = kml.newpolygon(outerboundaryis=list(geom.exterior.coords))
            else:
                continue
            schema = simplekml.SchemaData("kmlschema")
            for col in data.columns:
                if col != "geometry":
                    schema.newsimpledata(col, str(row[col]))
                    if col not in append_cols:
                        dtype_str = str(data[col].dtype)
                        if dtype_str == "object" or dtype_str == "bool":
                            dtype_str = "string"
                        elif dtype_str.startswith("int"):
                            dtype_str = "int"
                        elif dtype_str.startswith("float"):
                            dtype_str = "float"
                        append_cols[col] = dtype_str
            entry.extendeddata.schemadata = schema

        for col, typ in append_cols.items():
            kml_schema.newsimplefield(col, type=typ)

        kml.save(file_path)

        if not is_local:
            mode = "overwrite" if replace else "create"
            fs.put_file(file_path, data_id, mode=mode)
        return data_id
