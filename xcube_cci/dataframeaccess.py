# The MIT License (MIT)
# Copyright (c) 2024 ESA Climate Change Initiative
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
from typing import Mapping
from typing import Tuple
import geopandas as gpd
import pandas as pd
import shapely

from xcube_cci.ccicdc import CciCdc
from xcube_cci.timerangegetter import TimeRangeGetter


class DataFrameAccessor:

    def __init__(self, cci_cdc: CciCdc, df_id: str, gdf_params: Mapping[str, Any]):
        self._cci_cdc = cci_cdc
        self._df_id = df_id
        self._metadata = cci_cdc.get_dataset_metadata(df_id)
        self._var_names = gdf_params.get("variable_names", {}).copy()
        if len(self._var_names) == 0:
            self._var_names = list(self._metadata.get("variable_infos").keys())
        self._spatial_subset_area = None
        if "bbox" in gdf_params:
            min_lon = gdf_params["bbox"][0]
            min_lat = gdf_params["bbox"][1]
            max_lon = gdf_params["bbox"][2]
            max_lat = gdf_params["bbox"][3]
            coords = ((min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat),
                      (max_lon, min_lat), (min_lon, min_lat))
            self._spatial_subset_area = shapely.Polygon(coords)
        # we use a time chunking of 1 as we do not know the actual content of a nc file
        self._time_chunking = 1
        tr = TimeRangeGetter(self._cci_cdc, self._metadata)
        self._time_ranges = tr.get_time_ranges(self._df_id, gdf_params)

    def request_time_range(self, time_index: int) -> Tuple:
        start_index = time_index * self._time_chunking
        end_index = ((time_index + 1) * self._time_chunking) - 1
        start_time = self._time_ranges[start_index][0]
        end_time = self._time_ranges[end_index][1]
        return start_time, end_time

    def get_geodataframe(self) -> gpd.GeoDataFrame:
        features = self._get_features()
        gdf = GeoDataFrame.from_features(features)
        return gdf

    def _get_features(self):
        for i in range(len(self._time_ranges)):
            return self._get_features_from_cci_cdc(i)

    def _get_features_from_cci_cdc(self, index: int = 0) -> gpd.GeoDataFrame:
        identifier = self._cci_cdc.get_dataset_id(self._df_id)
        start_time, end_time = self.request_time_range(time_index=index)
        try:
            start_time = start_time.tz_localize(None).isoformat()
            end_time = end_time.tz_localize(None).isoformat()
        except TypeError:
            # use unconverted time range values
            pass
        var_infos = self._metadata.get("variable_infos")
        all_var_names = list(var_infos.keys())
        var_names = self._var_names
        spatial_coord_names = ["lat", "lon", "latitude", "longitude", "geometry"]
        for spatial_coord_name in spatial_coord_names:
            if spatial_coord_name in all_var_names and \
                    spatial_coord_name not in var_names:
                var_names.append(spatial_coord_name)
        if "time" in all_var_names:
            var_names.append("time")
        if self._metadata.get("attributes", {}).get("shapefile", False):
            request = dict(parentIdentifier=identifier,
                           varNames=var_names,
                           startDate=start_time,
                           endDate=end_time,
                           drsId=self._df_id,
                           fileFormat='.shp')
            gdf = self._cci_cdc.get_geodataframe_from_shapefile(request)
        else:
            gdf_data = {}
            for var_name in var_names:
                if len(var_infos.get(var_name).get("dimensions", [])) > 1:
                    continue
                request = dict(parentIdentifier=identifier,
                               varNames=[var_name],
                               startDate=start_time,
                               endDate=end_time,
                               drsId=self._df_id,
                               fileFormat='.nc')
                gdf_data[var_name] = list(self._cci_cdc.get_data_chunk(
                    request, dim_indexes=(), to_bytes=False
                ))
            shift = False
            for lon_var in ["longitude", "lon"]:
                if lon_var in var_names:
                    if max(gdf_data[lon_var]) > 180.0:
                        shift = True
            if 'longitude' in var_names and 'latitude' in var_names:
                gdf = gpd.GeoDataFrame(
                    gdf_data, geometry=gpd.points_from_xy(
                        gdf_data['longitude'], gdf_data['latitude']
                    )
                )
                gdf = gdf.drop(['longitude', 'latitude'], axis=1)
            else:
                gdf = gpd.GeoDataFrame(
                    gdf_data,
                    geometry=gpd.points_from_xy(gdf_data['lon'], gdf_data['lat'])
                )
                gdf = gdf.drop(['lon', 'lat'], axis=1)
            if shift:
                gdf["geometry"] = gdf["geometry"].translate(xoff=-180.0)
        if self._spatial_subset_area:
            gdf["geometry"] = gdf.intersection(self._spatial_subset_area)
            gdf = gdf[~gdf["geometry"].is_empty]
        time_units = \
            self._metadata.get("variable_infos", {}).get("time", {}).get("units")
        if time_units and "since" in time_units:
            time_unit, t_offset = time_units.split("since")
            time_offset = pd.Timestamp(t_offset)
            time_unit = time_unit.strip().lower()

            def create_timestamp(df, tu, to):
                return to + pd.Timedelta(value=df['time'], unit=tu)

            gdf['time'] = gdf.apply(
                create_timestamp, args=(time_unit, time_offset), axis=1
            )
        if 'time' in gdf:
            gdf['time'] = gdf['time'].astype(str)
        else:
            gdf = gdf.assign(time=start_time)
        for feature in gdf.iterfeatures():
            yield feature


# this class is originally from esa_climate_toolbox.core.types
# it is duplicated here to avoid circular imports
class GeoDataFrame:
    """
    Proxy for a ``geopandas.GeoDataFrame`` that holds an iterable of features
    or a feature collection for fastest possible streaming of GeoJSON features.

    Important note: although GeoDataFrame is not inherited from
    geopandas.GeoDataFrame it is an instance of that type meaning that::

        isinstance(GeoDataFrame(...), geopandas.GeoDataFrame) == True

    It also has all attributes of a geopandas.GeoDataFrame.
    This allows us using the proxy for operation parameters
    of type parameters pandas.DataFrame and geopandas.GeoDataFrame.
    """

    _OWN_PROPERTY_SET = {
        "_features",
        "features",
        "_lazy_data_frame",
        "lazy_data_frame",
        "close",
    }

    @classmethod
    def from_features(cls, features):
        """
        Create GeoDataFrame from an iterable of features
        or a feature collection.

        :param features: iterable of features or a feature collection,
            e.g., an open ``fiona.Collection`` instance.
        :return: An instance of a ``GeoDataFrame`` proxy.
        """
        return cls(features)

    def __init__(self, features):
        if features is None:
            raise ValueError('features must not be None')
        self._features = features
        self._lazy_data_frame = None

    @property
    def features(self):
        return self._features

    @property
    def lazy_data_frame(self):
        features = self._features
        if features is not None and self._lazy_data_frame is None:
            crs = features.crs if hasattr(features, 'crs') else None
            self._lazy_data_frame = gpd.GeoDataFrame.from_features(
                features, crs=crs
            )

        return self._lazy_data_frame

    def close(self):
        """
        In the ESA Climate Toolbox, closable resources are closed when removed
        from the resources cache. Therefore we provide a close method here,
        although geopandas.GeoDataFrame doesn't have one.
        """
        try:
            self._features.close()
        except AttributeError:
            pass
        self._features = None
        self._lazy_data_frame = None

    def __setattr__(self, key, value):
        if key in GeoDataFrame._OWN_PROPERTY_SET:
            object.__setattr__(self, key, value)
        else:
            self.lazy_data_frame.__setattr__(key, value)

    def __getattribute__(self, item):
        if item in GeoDataFrame._OWN_PROPERTY_SET:
            return object.__getattribute__(self, item)
        else:
            return getattr(self.lazy_data_frame, item)

    def __getattr__(self, item):
        if item in GeoDataFrame._OWN_PROPERTY_SET:
            return object.__getattribute__(self, item)
        else:
            return getattr(self.lazy_data_frame, item)

    def __getitem__(self, item):
        return self.lazy_data_frame.__getitem__(item)

    def __setitem__(self, key, value):
        return self.lazy_data_frame.__setitem__(key, value)

    def __str__(self):
        return str(self.lazy_data_frame)

    def __repr__(self):
        return repr(self.lazy_data_frame)

    def __len__(self):
        return len(self._features)
