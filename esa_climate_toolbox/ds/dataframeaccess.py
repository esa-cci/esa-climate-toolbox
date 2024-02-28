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

from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union
import geopandas as gpd
import pandas as pd
import shapely

from ..constants import MONTHS
from .constants import TIMESTAMP_FORMAT
from esa_climate_toolbox.ds.ccicdc import CciCdc
from esa_climate_toolbox.core.types import GeoDataFrame


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
        self._time_ranges = self._get_time_ranges(self._df_id, gdf_params)

    def _get_time_ranges(self, dataset_id: str,
                         params: Mapping[str, Any]) -> List[Tuple]:
        start_time, end_time, iso_start_time, iso_end_time = \
            self._extract_time_range_as_datetime(
                params.get('time_range',
                           self.get_default_time_range(dataset_id)))
        time_period = dataset_id.split('.')[2]
        if time_period == 'day':
            start_time = datetime(year=start_time.year, month=start_time.month,
                                  day=start_time.day)
            end_time = datetime(year=end_time.year, month=end_time.month,
                                day=end_time.day,
                                hour=23, minute=59, second=59)
            delta = relativedelta(days=1)
        elif time_period == 'month' or time_period == 'mon':
            start_time = datetime(year=start_time.year, month=start_time.month,
                                  day=1)
            end_time = datetime(year=end_time.year, month=end_time.month, day=1)
            delta = relativedelta(months=1)
            end_time += delta
        elif time_period == 'year' or time_period == 'yr':
            start_time = datetime(year=start_time.year, month=1, day=1)
            end_time = datetime(year=end_time.year, month=12, day=31)
            delta = relativedelta(years=1)
        elif time_period == 'climatology':
            return [(i + 1, i + 1) for i, month in enumerate(MONTHS)]
        else:
            end_time = end_time.replace(hour=23, minute=59, second=59)
            end_time_str = datetime.strftime(end_time, TIMESTAMP_FORMAT)
            iso_end_time = self._extract_time_as_string(end_time_str)
            request_time_ranges = self._cci_cdc.get_time_ranges_from_data(
                dataset_id, iso_start_time, iso_end_time)
            return request_time_ranges
        request_time_ranges = []
        this = start_time
        while this < end_time:
            after = this + delta
            pd_this = pd.Timestamp(datetime.strftime(this, TIMESTAMP_FORMAT))
            pd_next = pd.Timestamp(datetime.strftime(after, TIMESTAMP_FORMAT))
            request_time_ranges.append((pd_this, pd_next))
            this = after
        return request_time_ranges

    def _extract_time_range_as_datetime(
            self, time_range: Union[Tuple, List]
    ) -> (datetime, datetime, str, str):
        iso_start_time, iso_end_time = self._extract_time_range_as_strings(
            time_range)
        start_time = datetime.strptime(iso_start_time, TIMESTAMP_FORMAT)
        end_time = datetime.strptime(iso_end_time, TIMESTAMP_FORMAT)
        return start_time, end_time, iso_start_time, iso_end_time

    @classmethod
    def _extract_time_range_as_strings(
            cls, time_range: Union[Tuple, List]
    ) -> (str, str):
        if isinstance(time_range, tuple):
            time_start, time_end = time_range
        else:
            time_start = time_range[0]
            time_end = time_range[1]
        return \
            cls._extract_time_as_string(time_start),\
            cls._extract_time_as_string(time_end)

    @classmethod
    def _extract_time_as_string(
            cls, time_value: Union[pd.Timestamp, str]
    ) -> str:
        if isinstance(time_value, str):
            time_value = pd.to_datetime(time_value, utc=True)
        return time_value.tz_localize(None).isoformat()

    def get_default_time_range(self, ds_id: str):
        temporal_start = self._metadata.get('temporal_coverage_start', None)
        temporal_end = self._metadata.get('temporal_coverage_end', None)
        if not temporal_start or not temporal_end:
            time_ranges = self._cci_cdc.get_time_ranges_from_data(ds_id)
            if not temporal_start:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal start of dataset. "
                        "Please use 'time_range' parameter."
                    )
                temporal_start = time_ranges[0][0]
            if not temporal_end:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal end of dataset. "
                        "Please use 'time_range' parameter."
                    )
                temporal_end = time_ranges[-1][1]
        return temporal_start, temporal_end

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
        spatial_coord_names = ["lat", "lon", "latitude", "longitude"]
        for spatial_coord_name in spatial_coord_names:
            if spatial_coord_name in all_var_names and \
                    spatial_coord_name not in var_names:
                var_names.append(spatial_coord_name)
        gdf_data = {}
        if "time" in all_var_names:
            var_names.append("time")
        for var_name in var_names:
            if len(var_infos.get(var_name).get("dimensions", 1)) > 1:
                continue
            request = dict(parentIdentifier=identifier,
                           varNames=[var_name],
                           startDate=start_time,
                           endDate=end_time,
                           drsId=self._df_id,
                           fileFormat='.nc'
                           )
            gdf_data[var_name] = list(self._cci_cdc.get_data_chunk(
                request, dim_indexes=(), to_bytes=False
            ))
        if 'longitude' in var_names and 'latitude' in var_names:
            gdf = gpd.GeoDataFrame(
                gdf_data,
                geometry=gpd.points_from_xy(gdf_data['longitude'], gdf_data['latitude'])
            )
            gdf = gdf.drop(['longitude', 'latitude'], axis=1)
        else:
            gdf = gpd.GeoDataFrame(
                gdf_data,
                geometry=gpd.points_from_xy(gdf_data['lon'], gdf_data['lat'])
            )
            gdf = gdf.drop(['lon', 'lat'], axis=1)
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
            gdf['time'] = gdf['time'].astype(str)
        for feature in gdf.iterfeatures():
            yield feature
