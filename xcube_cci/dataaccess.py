# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
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

from abc import abstractmethod
import json
import os
from typing import Any, Iterator, List, Tuple, Optional, Dict, Union, Container
import pyproj
from shapely import Point
import xarray as xr
import xvec

from xcube.core.normalize import normalize_dataset
from xcube.core.store import GEO_DATA_FRAME_TYPE
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import DataType
from xcube.core.store import DatasetDescriptor
from xcube.core.store import GeoDataFrameDescriptor
from xcube.core.store import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema

from .cciodp import CciCdc
from .chunkstore import CciChunkStore
from .constants import CCI_ODD_URL
from .constants import DATAFRAME_OPENER_ID
from .constants import DATASET_OPENER_ID
from .constants import DEFAULT_NUM_RETRIES
from .constants import DEFAULT_RETRY_BACKOFF_BASE
from .constants import DEFAULT_RETRY_BACKOFF_MAX
from .constants import OPENSEARCH_CEDA_URL
from .constants import VECTORDATACUBE_OPENER_ID
from .dataframeaccess import DataFrameAccessor
from .normalize import normalize_coord_names
from .normalize import normalize_dims_description
from .normalize import normalize_variable_dims_description
from .normalize import normalize_var_infos
from .vdcaccess import VectorDataCubeDescriptor
from .vdcaccess import VECTOR_DATA_CUBE_TYPE

_RELEVANT_METADATA_ATTRIBUTES = \
    ['ecv', 'institute', 'processing_level', 'product_string',
     'product_version', 'data_type', 'abstract', 'title', 'licences',
     'publication_date', 'catalog_url', 'sensor_id', 'platform_id',
     'cci_project', 'description', 'project', 'references', 'source',
     'history', 'comment', 'uuid']
_INSTITUTES = ['Alfred-Wegener-Institut Helmholtz-Zentrum für '
               'Polar- und Meeresforschung', 'Plymouth Marine Laboratory',
               'ENVironmental Earth Observation IT GmbH',
               'multi-institution', 'DTU Space', 'Freie Universitaet Berlin',
               'Vienna University of Technology', 'Deutscher Wetterdienst',
               'Netherlands Institute for Space Research',
               'Technische Universität Dresden',
               'Institute of Environmental Physics',
               'Rutherford Appleton Laboratory',
               'Universite Catholique de Louvain', 'University of Alcala',
               'University of Leicester', 'Norwegian Meteorological Institute',
               'University of Bremen', 'Belgian Institute for Space Aeronomy',
               'Deutsches Zentrum fuer Luft- und Raumfahrt',
               'Royal Netherlands Meteorological Institute',
               'The Geological Survey of Denmark and Greenland']


def get_temporal_resolution_from_id(data_id: str) -> Optional[str]:
    data_time_res = data_id.split('.')[2]
    time_res_items = dict(D=['days', 'day'],
                          M=['months', 'mon', 'climatology'],
                          Y=['yrs', 'yr', 'year'])
    for time_res_pandas_id, time_res_ids_list in time_res_items.items():
        for i, time_res_id in enumerate(time_res_ids_list):
            if time_res_id in data_time_res:
                if i == 0:
                    return f'{data_time_res.split("-")[0]}{time_res_pandas_id}'
                return f'1{time_res_pandas_id}'


class CciCdcDataOpener(DataOpener):

    # noinspection PyShadowingBuiltins
    def __init__(self,
                 cci_cdc: CciCdc,
                 id: str,
                 data_type: DataType,
                 normalize_data: bool = True):
        self._cci_cdc = cci_cdc
        self._id = id
        self._data_type = data_type
        self._normalize_data = normalize_data

    @property
    def dataset_names(self) -> List[str]:
        return self._cci_cdc.dataset_names

    @classmethod
    @abstractmethod
    def get_data_types(cls) -> Tuple[DataType, ...]:
        pass

    def has_data(self, data_id: str) -> bool:
        return data_id in self.dataset_names

    def _describe_data(self, data_ids: List[str]) -> List[DatasetDescriptor]:
        ds_metadata_list = self._cci_cdc.get_datasets_metadata(data_ids)
        data_descriptors = []
        for i, ds_metadata in enumerate(ds_metadata_list):
            data_descriptors.append(self._get_data_descriptor_from_metadata(
                data_ids[i], ds_metadata)
            )
        return data_descriptors

    def describe_data(self, data_id: str) -> DataDescriptor:
        self._assert_valid_data_id(data_id)
        try:
            ds_metadata = self._cci_cdc.get_dataset_metadata(data_id)
            return self._get_data_descriptor_from_metadata(data_id, ds_metadata)
        except ValueError:
            raise DataStoreError(
                f'Cannot describe metadata. '
                f'"{data_id}" does not seem to be a valid identifier.'
            )

    # noinspection PyArgumentList
    @abstractmethod
    def _get_data_descriptor_from_metadata(self,
                                           data_id: str,
                                           metadata: dict) -> DatasetDescriptor:
        pass

    def _get_variable_descriptors(self,
                                  var_names: List[str],
                                  var_infos: dict,
                                  time_dim_name: str,
                                  normalize_dims: bool = True) \
            -> Dict[str, VariableDescriptor]:
        var_descriptors = {}
        for var_name in var_names:
            if var_name in var_infos:
                var_info = var_infos[var_name]
                var_dtype = var_info['data_type']
                var_dims = self._normalize_var_dims(
                    var_info['dimensions'], time_dim_name) \
                    if normalize_dims else var_info['dimensions']
                var_descriptors[var_name] = VariableDescriptor(
                    var_name, var_dtype, var_dims, attrs=var_info
                )
            else:
                var_descriptors[var_name] = VariableDescriptor(
                    var_name, '', ''
                )
        return var_descriptors

    @staticmethod
    def _remove_irrelevant_metadata_attributes(attrs: dict):
        to_remove_list = []
        for attribute in attrs:
            if attribute not in _RELEVANT_METADATA_ATTRIBUTES:
                to_remove_list.append(attribute)
        for to_remove in to_remove_list:
            attrs.pop(to_remove)

    def search_data(self, **search_params) -> Iterator[DatasetDescriptor]:
        search_result = self._cci_cdc.search(**search_params)
        data_descriptors = self._describe_data(search_result)
        return iter(data_descriptors)

    def get_open_data_params_schema(
            self, data_id: str = None
    ) -> JsonObjectSchema:
        if data_id is None:
            return self._get_open_data_params_schema()
        self._assert_valid_data_id(data_id)
        dsd = self.describe_data(data_id)
        return self._get_open_data_params_schema(dsd)

    @abstractmethod
    def _get_open_data_params_schema(
            self,
            dsd: DatasetDescriptor = None
    ) -> JsonObjectSchema:
        pass

    @abstractmethod
    def open_data(self, data_id: str, **open_params) -> Any:
        pass

    def _assert_valid_data_id(self, data_id: str):
        if data_id not in self.dataset_names:
            raise DataStoreError(f'Cannot describe metadata of '
                                 f'data resource "{data_id}", '
                                 f'as it cannot be accessed by '
                                 f'data accessor "{self._id}".')

    def _normalize_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        if self._normalize_data:
            return normalize_dataset(ds)
        return ds

    def _normalize_dims(self, dims: dict) -> dict:
        if self._normalize_data:
            return normalize_dims_description(dims)
        return dims.copy()

    def _normalize_var_dims(self, var_dims: List[str], time_dim_name: str) \
            -> Optional[List[str]]:
        if self._normalize_data:
            return normalize_variable_dims_description(var_dims)
        new_var_dims = var_dims.copy()
        if time_dim_name not in new_var_dims and len(new_var_dims) > 0:
            new_var_dims.insert(0, time_dim_name)
        return new_var_dims

    def _normalize_var_infos(self, var_infos: Dict[str, Dict[str, Any]]) -> \
            Dict[str, Dict[str, Any]]:
        if self._normalize_data:
            return normalize_var_infos(var_infos.copy())
        return var_infos

    def _normalize_coord_names(self, coord_names: List[str]):
        if self._normalize_data:
            return normalize_coord_names(coord_names.copy())
        return coord_names


class CciCdcDatasetOpener(CciCdcDataOpener):

    def __init__(self, normalize_data: bool = True, **cdc_params):
        super().__init__(
            CciCdc(**cdc_params, data_type='dataset'),
            DATASET_OPENER_ID,
            DATASET_TYPE,
            normalize_data=normalize_data,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[DataType, ...]:
        return DATASET_TYPE,

    def _get_data_descriptor_from_metadata(self,
                                           data_id: str,
                                           metadata: dict) -> DatasetDescriptor:
        ds_metadata = metadata.copy()
        is_climatology = \
            ds_metadata.get('time_frequency', '') == 'climatology' and \
            'AEROSOL' in data_id
        dims = self._normalize_dims(ds_metadata.get('dimensions', {}))
        bounds_dim_name = None
        for dim_name, dim_size in dims.items():
            if dim_size == 2:
                bounds_dim_name = dim_name
                break
        if not bounds_dim_name and not is_climatology:
            bounds_dim_name = 'bnds'
            dims['bnds'] = 2
        temporal_resolution = get_temporal_resolution_from_id(data_id)
        dataset_info = self._cci_cdc.get_dataset_info(data_id, ds_metadata)
        spatial_resolution = dataset_info['y_res']
        if spatial_resolution <= 0:
            spatial_resolution = None
        bbox = dataset_info['bbox']
        crs = dataset_info['crs']
        # only use date parts of times
        if is_climatology:
            temporal_coverage = None
        else:
            temporal_coverage = (
                dataset_info['temporal_coverage_start'].split('T')[0]
                if dataset_info['temporal_coverage_start'] else None,
                dataset_info['temporal_coverage_end'].split('T')[0]
                if dataset_info['temporal_coverage_end'] else None
            )
        var_infos = self._normalize_var_infos(
            ds_metadata.get('variable_infos', {})
        )
        coord_names = self._normalize_coord_names(dataset_info['coord_names'])

        time_dim_name = "time"
        if is_climatology:
            time_dim_name = 'month'
        if "Time" in dims.keys():
            time_dim_name = "Time"
        var_descriptors = self._get_variable_descriptors(
            dataset_info['var_names'], var_infos, time_dim_name
        )
        coord_descriptors = self._get_variable_descriptors(coord_names,
                                                           var_infos,
                                                           time_dim_name,
                                                           normalize_dims=False)
        if time_dim_name not in coord_descriptors.keys() and \
                't' not in coord_descriptors.keys():
            if is_climatology:
                coord_descriptors[time_dim_name] = VariableDescriptor(
                    time_dim_name, dtype='int8', dims=('time_dim_name',)
                )
            else:
                time_attrs = {
                    "units": "seconds since 1970-01-01T00:00:00Z",
                    "calendar": "proleptic_gregorian",
                    "standard_name": "time",
                    "shape": [dims[time_dim_name]],
                    "size": dims[time_dim_name]
                }
                coord_descriptors[time_dim_name] = VariableDescriptor(
                    time_dim_name, dtype='int64', dims=(time_dim_name,),
                    attrs=time_attrs
                )
                if 'time_bnds' in coord_descriptors.keys():
                    coord_descriptors.pop('time_bnds')
                if 'time_bounds' in coord_descriptors.keys():
                    coord_descriptors.pop('time_bounds')
                time_bnds_attrs = {
                    "units": "seconds since 1970-01-01T00:00:00Z",
                    "calendar": "proleptic_gregorian",
                    "standard_name": "time_bnds",
                    "shape": [2, dims[time_dim_name]],
                    "size": 2 * dims[time_dim_name]
                }
                coord_descriptors['time_bnds'] = \
                    VariableDescriptor('time_bnds',
                                       dtype='int64',
                                       dims=(time_dim_name, bounds_dim_name),
                                       attrs=time_bnds_attrs)

        if 'variables' in ds_metadata:
            ds_metadata.pop('variables')
        ds_metadata.pop('dimensions')
        ds_metadata.pop('variable_infos')
        attrs = ds_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        ds_metadata.pop('attributes')
        attrs.update(ds_metadata)
        self._remove_irrelevant_metadata_attributes(attrs)
        descriptor = DatasetDescriptor(data_id,
                                       data_type=self._data_type,
                                       crs=crs,
                                       dims=dims,
                                       coords=coord_descriptors,
                                       data_vars=var_descriptors,
                                       attrs=attrs,
                                       bbox=bbox,
                                       spatial_res=spatial_resolution,
                                       time_range=temporal_coverage,
                                       time_period=temporal_resolution)
        data_schema = self._get_open_data_params_schema(descriptor)
        descriptor.open_params_schema = data_schema
        return descriptor

    def open_data(self, data_id: str, **open_params) -> Any:
        cci_schema = self.get_open_data_params_schema(data_id)
        cci_schema.validate_instance(open_params)
        cube_kwargs, open_params = cci_schema.process_kwargs_subset(
            open_params, ('variable_names',
                          'time_range',
                          'bbox')
        )
        chunk_store = CciChunkStore(self._cci_cdc, data_id, cube_kwargs)
        ds = xr.open_zarr(chunk_store, consolidated=False)
        ds.zarr_store.set(chunk_store)
        ds = self._normalize_dataset(ds)
        return ds

    def _get_open_data_params_schema(
            self, dsd: DatasetDescriptor = None
    ) -> JsonObjectSchema:
        # noinspection PyUnresolvedReferences
        dataset_params = dict(
            normalize_data=JsonBooleanSchema(default=True),
            variable_names=JsonArraySchema(items=JsonStringSchema(
                enum=dsd.data_vars.keys() if dsd and dsd.data_vars else None))
        )
        if dsd:
            min_date = dsd.time_range[0] if dsd.time_range else None
            max_date = dsd.time_range[1] if dsd.time_range else None
            if min_date or max_date:
                dataset_params['time_range'] = \
                    JsonDateSchema.new_range(min_date, max_date)
        else:
            dataset_params['time_range'] = JsonDateSchema.new_range(None, None)
        if dsd:
            try:
                if pyproj.CRS.from_string(dsd.crs).is_geographic:
                    min_lon = dsd.bbox[0] if dsd and dsd.bbox else -180
                    min_lat = dsd.bbox[1] if dsd and dsd.bbox else -90
                    max_lon = dsd.bbox[2] if dsd and dsd.bbox else 180
                    max_lat = dsd.bbox[3] if dsd and dsd.bbox else 90
                    bbox = JsonArraySchema(items=(
                        JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                        JsonNumberSchema(minimum=min_lat, maximum=max_lat),
                        JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                        JsonNumberSchema(minimum=min_lat, maximum=max_lat)))
                    dataset_params['bbox'] = bbox
            except pyproj.exceptions.CRSError:
                # do not set bbox then
                pass
        cci_schema = JsonObjectSchema(
            properties=dict(**dataset_params),
            required=[
            ],
            additional_properties=False
        )
        return cci_schema


class CciCdcDataFrameOpener(CciCdcDataOpener):

    def __init__(self, **cdc_params):
        super().__init__(
            CciCdc(**cdc_params, data_type='geodataframe'),
            DATAFRAME_OPENER_ID,
            GEO_DATA_FRAME_TYPE
        )

    @classmethod
    def get_data_types(cls) -> Tuple[DataType, ...]:
        return GEO_DATA_FRAME_TYPE,

    def _get_data_descriptor_from_metadata(self,
                                           data_id: str,
                                           metadata: dict) -> GeoDataFrameDescriptor:
        ds_metadata = metadata.copy()
        temporal_resolution = get_temporal_resolution_from_id(data_id)
        dataset_info = self._cci_cdc.get_dataset_info(data_id, ds_metadata)
        bbox = dataset_info['bbox']
        crs = dataset_info['crs']
        temporal_coverage = (
            dataset_info['temporal_coverage_start'].split('T')[0]
            if dataset_info['temporal_coverage_start'] else None,
            dataset_info['temporal_coverage_end'].split('T')[0]
            if dataset_info['temporal_coverage_end'] else None
        )
        feature_schema = self._get_feature_schema(ds_metadata)
        descriptor = GeoDataFrameDescriptor(data_id,
                                            data_type=self._data_type,
                                            crs=crs,
                                            bbox=bbox,
                                            time_range=temporal_coverage,
                                            time_period=temporal_resolution,
                                            feature_schema=feature_schema)
        data_schema = self._get_open_data_params_schema(descriptor)
        descriptor.open_params_schema = data_schema
        return descriptor

    @staticmethod
    def _get_feature_schema(metadata: dict):
        int_data_types = ['uint8', 'uint16', 'uint32', 'int8', 'int16', 'int32']
        features = dict()
        for var_name, var_dict in metadata.get("variable_infos", {}).items():
            dt = var_dict.get("datatype")
            if var_name in ["lat", "lon", "latitude", "longitude"]:
                continue
            if dt == "float32" or dt == "float64":
                features[var_name] = JsonNumberSchema()
            elif dt in int_data_types:
                features[var_name] = JsonIntegerSchema()
            else:
                features[var_name] = JsonObjectSchema()
        features["time"] = JsonDateSchema()
        features["geometry"] = JsonStringSchema(pattern="POINT(* *)")
        feature_schema = JsonObjectSchema(
            properties=dict(**features),
            required=[
            ],
            additional_properties=False
        )
        return feature_schema

    def open_data(self, data_id: str, **open_params) -> Any:
        cci_schema = self.get_open_data_params_schema(data_id)
        cci_schema.validate_instance(open_params)
        frame_kwargs, open_params = cci_schema.process_kwargs_subset(
            open_params, ('variable_names',
                          'time_range',
                          'bbox')
        )
        data_frame_accessor = DataFrameAccessor(self._cci_cdc, data_id, frame_kwargs)
        return data_frame_accessor.get_geodataframe()

    def _get_open_data_params_schema(
            self,
            gfd: GeoDataFrameDescriptor = None
    ) -> JsonObjectSchema:
        # noinspection PyUnresolvedReferences
        geodataframe_params = dict()
        if gfd:
            feature_schema = gfd.feature_schema.to_dict()
            var_names = list(feature_schema.get("properties", {}).keys())
            var_names.remove("geometry")
            var_names.remove("time")
            geodataframe_params["variable_names"] = JsonArraySchema(
                items=JsonStringSchema(enum=var_names)
            )
        if gfd:
            min_date = gfd.time_range[0] if gfd.time_range else None
            max_date = gfd.time_range[1] if gfd.time_range else None
            if min_date or max_date:
                geodataframe_params['time_range'] = \
                    JsonDateSchema.new_range(min_date, max_date)
        else:
            geodataframe_params['time_range'] = JsonDateSchema.new_range(None, None)
        if gfd:
            try:
                if pyproj.CRS.from_string(gfd.crs).is_geographic:
                    min_lon = gfd.bbox[0] if gfd and gfd.bbox else -180
                    min_lat = gfd.bbox[1] if gfd and gfd.bbox else -90
                    max_lon = gfd.bbox[2] if gfd and gfd.bbox else 180
                    max_lat = gfd.bbox[3] if gfd and gfd.bbox else 90
                    bbox = JsonArraySchema(items=(
                        JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                        JsonNumberSchema(minimum=min_lat, maximum=max_lat),
                        JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                        JsonNumberSchema(minimum=min_lat, maximum=max_lat))
                    )
                    geodataframe_params['bbox'] = bbox
            except pyproj.exceptions.CRSError:
                # do not set bbox then
                pass
        cci_schema = JsonObjectSchema(
            properties=dict(**geodataframe_params),
            required=[
            ],
            additional_properties=False
        )
        return cci_schema


class CciCdcVectorDataCubeOpener(CciCdcDataOpener):

    def __init__(self, normalize_data: bool = True, **cdc_params):
        super().__init__(
            CciCdc(**cdc_params, data_type='vectordatacube'),
            VECTORDATACUBE_OPENER_ID,
            VECTOR_DATA_CUBE_TYPE,
            normalize_data=normalize_data,
        )

    @classmethod
    def get_data_types(cls) -> Tuple[DataType, ...]:
        return VECTOR_DATA_CUBE_TYPE,

    def _get_data_descriptor_from_metadata(
            self, data_id: str, metadata: dict
    ) -> VectorDataCubeDescriptor:
        ds_metadata = metadata.copy()
        dims = self._normalize_dims(ds_metadata.get('dimensions', {}))
        temporal_resolution = get_temporal_resolution_from_id(data_id)
        dataset_info = self._cci_cdc.get_dataset_info(data_id, ds_metadata)
        bbox = dataset_info['bbox']
        crs = dataset_info['crs']
        temporal_coverage = (
            dataset_info['temporal_coverage_start'].split('T')[0]
            if dataset_info['temporal_coverage_start'] else None,
            dataset_info['temporal_coverage_end'].split('T')[0]
            if dataset_info['temporal_coverage_end'] else None
        )
        var_infos = self._normalize_var_infos(
            ds_metadata.get('variable_infos', {})
        )
        var_names = dataset_info["var_names"]
        lat_lons = [("lat", "lon"), ("latitude", "longitude")]
        for lat_lon in lat_lons:
            if lat_lon[0] in var_names and lat_lon[1] in var_names:
                var_names.remove(lat_lon[0])
                var_names.remove(lat_lon[1])
            break
        coord_names = self._normalize_coord_names(dataset_info['coord_names'])
        time_dim_name = 'time'
        var_descriptors = self._get_variable_descriptors(
            dataset_info['var_names'], var_infos, time_dim_name
        )
        coord_descriptors = self._get_variable_descriptors(coord_names,
                                                           var_infos,
                                                           time_dim_name,
                                                           normalize_dims=False)
        if 'variables' in ds_metadata:
            ds_metadata.pop('variables')
        ds_metadata.pop('dimensions')
        ds_metadata.pop('variable_infos')
        attrs = ds_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        ds_metadata.pop('attributes')
        attrs.update(ds_metadata)
        self._remove_irrelevant_metadata_attributes(attrs)
        descriptor = VectorDataCubeDescriptor(data_id,
                                              data_type=self._data_type,
                                              crs=crs,
                                              dims=dims,
                                              coords=coord_descriptors,
                                              data_vars=var_descriptors,
                                              attrs=attrs,
                                              bbox=bbox,
                                              time_range=temporal_coverage,
                                              time_period=temporal_resolution)
        data_schema = self._get_open_data_params_schema(descriptor)
        descriptor.open_params_schema = data_schema
        return descriptor

    def _get_open_data_params_schema(
            self, vdcd: VectorDataCubeDescriptor = None
    ) -> JsonObjectSchema:
        # noinspection PyUnresolvedReferences
        vectordatacube_params = dict(
            normalize_data=JsonBooleanSchema(default=True),
            variable_names=JsonArraySchema(items=JsonStringSchema(
                enum=vdcd.data_vars.keys() if vdcd and vdcd.data_vars else None))
        )
        # TODO add this when spatial and temporal subsetting are supported
        # if vdcd:
        #     min_date = vdcd.time_range[0] if vdcd.time_range else None
        #     max_date = vdcd.time_range[1] if vdcd.time_range else None
        #     if min_date or max_date:
        #         vectordatacube_params['time_range'] = \
        #             JsonDateSchema.new_range(min_date, max_date)
        # else:
        #     vectordatacube_params['time_range'] = JsonDateSchema.new_range(None, None)
        # if vdcd:
        #     try:
        #         if pyproj.CRS.from_string(vdcd.crs).is_geographic:
        #             min_lon = vdcd.bbox[0] if vdcd and vdcd.bbox else -180
        #             min_lat = vdcd.bbox[1] if vdcd and vdcd.bbox else -90
        #             max_lon = vdcd.bbox[2] if vdcd and vdcd.bbox else 180
        #             max_lat = vdcd.bbox[3] if vdcd and vdcd.bbox else 90
        #             bbox = JsonArraySchema(items=(
        #                 JsonNumberSchema(minimum=min_lon, maximum=max_lon),
        #                 JsonNumberSchema(minimum=min_lat, maximum=max_lat),
        #                 JsonNumberSchema(minimum=min_lon, maximum=max_lon),
        #                 JsonNumberSchema(minimum=min_lat, maximum=max_lat)))
        #             vectordatacube_params['bbox'] = bbox
        #     except pyproj.exceptions.CRSError:
                # do not set bbox then
                # pass
        cci_schema = JsonObjectSchema(
            properties=dict(**vectordatacube_params),
            required=[
            ],
            additional_properties=False
        )
        return cci_schema

    def open_data(self, data_id: str, **open_params) -> Any:
        cci_schema = self.get_open_data_params_schema(data_id)
        cci_schema.validate_instance(open_params)
        cube_kwargs, open_params = cci_schema.process_kwargs_subset(
            open_params, ('variable_names',
                          'time_range',
                          'bbox')
        )
        chunk_store = CciChunkStore(self._cci_cdc, data_id, cube_kwargs)
        ds = xr.open_zarr(chunk_store, consolidated=False)

        def _convert_to_point(chunk):
            return [Point(point_dict.get("coordinates")) for point_dict in chunk]

        da = xr.apply_ufunc(_convert_to_point, ds.geometry,
                            dask='parallelized', output_dtypes=["object"])
        ds = ds.assign_coords(geometry=da)
        ds = ds.set_xindex("geometry", xvec.GeometryIndex)
        ds.zarr_store.set(chunk_store)
        return ds


class CciCdcDataStore(DataStore):

    def __init__(self, normalize_data=True, **store_params):
        cci_schema = self.get_data_store_params_schema()
        cci_schema.validate_instance(store_params)
        cdc_kwargs, store_params = cci_schema.process_kwargs_subset(
            store_params, ('endpoint_url', 'endpoint_description_url',
                           'enable_warnings', 'num_retries',
                           'retry_backoff_max', 'retry_backoff_base',
                           'user_agent')
        )
        dataset_opener = CciCdcDatasetOpener(
            normalize_data=normalize_data,
            **cdc_kwargs
        )
        dataframe_opener = CciCdcDataFrameOpener(
            **cdc_kwargs
        )
        vectordatacube_opener = CciCdcVectorDataCubeOpener(
            **cdc_kwargs
        )
        self._openers = {
            DATASET_OPENER_ID: dataset_opener,
            DATAFRAME_OPENER_ID: dataframe_opener,
            VECTORDATACUBE_OPENER_ID: vectordatacube_opener
        }
        dataset_states_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data/dataset_states.json'
        )
        with open(dataset_states_file, 'r') as fp:
            self._dataset_states = json.load(fp)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        ccicdc_params = dict(
            endpoint_url=JsonStringSchema(default=OPENSEARCH_CEDA_URL),
            endpoint_description_url=JsonStringSchema(default=CCI_ODD_URL),
            enable_warnings=JsonBooleanSchema(
                default=False, title='Whether to output warnings'
            ),
            num_retries=JsonIntegerSchema(
                default=DEFAULT_NUM_RETRIES, minimum=0,
                title='Number of retries when requesting data fails'
            ),
            retry_backoff_max=JsonIntegerSchema(
                default=DEFAULT_RETRY_BACKOFF_MAX, minimum=0
            ),
            retry_backoff_base=JsonNumberSchema(
                default=DEFAULT_RETRY_BACKOFF_BASE, exclusive_minimum=1.0
            ),
            user_agent=JsonStringSchema(default=None)
        )
        return JsonObjectSchema(
            properties=dict(**ccicdc_params),
            required=None,
            additional_properties=False
        )

    @classmethod
    def get_data_types(cls) -> Tuple[str, ...]:
        return (DATASET_TYPE.alias, GEO_DATA_FRAME_TYPE.alias,
                VECTOR_DATA_CUBE_TYPE.alias)

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        if self.has_data(data_id, data_type=DATASET_TYPE.alias):
            return DATASET_TYPE.alias,
        if self.has_data(data_id, data_type=GEO_DATA_FRAME_TYPE.alias):
            return GEO_DATA_FRAME_TYPE.alias,
        if self.has_data(data_id, data_type=VECTOR_DATA_CUBE_TYPE.alias):
            return VECTOR_DATA_CUBE_TYPE.alias,
        raise DataStoreError(
            f'Data resource {data_id!r} does not exist in store'
        )

    def _get_openers(
            self, opener_id: str = None, data_type: str = None
    ) -> List[CciCdcDataOpener]:
        self._assert_valid_opener_id(opener_id)
        try:
            self._assert_valid_data_type(data_type)
        except DataStoreError:
            return []
        if opener_id is not None:
            opener = self._openers[opener_id]
            if data_type is not None:
                opener_types = opener.get_data_types()
                for opener_type in opener_types:
                    if opener_type.is_super_type_of(data_type):
                        return [opener]
                raise ValueError(f"Opener '{opener_id}' is not compatible with "
                                 f"data type '{data_type}'.")
        if data_type is not None:
            if data_type == "vectordatacube":
                return [self._openers[VECTORDATACUBE_OPENER_ID]]
            if DATASET_TYPE.is_super_type_of(data_type):
                return [self._openers[DATASET_OPENER_ID]]
            if GEO_DATA_FRAME_TYPE.is_super_type_of(data_type):
                return [self._openers[DATAFRAME_OPENER_ID]]
        return list(self._openers.values())

    def get_data_ids(self,
                     data_type: str = None,
                     include_attrs: Container[str] = None) -> \
            Union[Iterator[str], Iterator[Tuple[str, Dict[str, Any]]]]:
        openers = self._get_openers(data_type=data_type)
        data_ids = []
        for opener in openers:
            data_ids.extend(opener.dataset_names)
        for data_id in data_ids:
            if include_attrs is None:
                yield data_id
            else:
                attrs = {}
                for attr in include_attrs:
                    value = self._dataset_states.get(data_id, {}).get(attr)
                    if value is not None:
                        attrs[attr] = value
                yield data_id, attrs

    def has_data(self, data_id: str, data_type: str = None) -> bool:
        openers = self._get_openers(data_type=data_type)
        for opener in openers:
            if opener.has_data(data_id):
                return True
        return False

    def describe_data(
            self, data_id: str, data_type: str = None
    ) -> DataDescriptor:
        openers = self._get_openers(data_type=data_type)
        for opener in openers:
            if opener.has_data(data_id):
                return opener.describe_data(data_id)
        raise ValueError(f"No opener provides data with id {data_id}")

    @classmethod
    def get_search_params_schema(
            cls, data_type: str = None
    ) -> JsonObjectSchema:
        cls._assert_valid_data_type(data_type)
        if data_type is not None:
            data_ids = CciCdc(data_type=data_type).dataset_names
        else:
            data_ids = CciCdc(data_type=DATASET_TYPE.alias).dataset_names + \
                       CciCdc(data_type=GEO_DATA_FRAME_TYPE.alias).dataset_names + \
                       CciCdc(data_type=VECTOR_DATA_CUBE_TYPE.alias).dataset_names
        ecvs = set([data_id.split('.')[1] for data_id in data_ids])
        frequencies = set(
            [data_id.split('.')[2].replace('-days', ' days').
                 replace('mon', 'month').replace('-yrs', ' years').
                 replace('yr', 'year') for data_id in data_ids]
        )
        processing_levels = set([data_id.split('.')[3] for data_id in data_ids])
        data_types = set([data_id.split('.')[4] for data_id in data_ids])
        sensors = set([data_id.split('.')[5] for data_id in data_ids])
        platforms = set([data_id.split('.')[6] for data_id in data_ids])
        product_strings = set([data_id.split('.')[7] for data_id in data_ids])
        product_versions = set([data_id.split('.')[8].replace('-', '.')
                                for data_id in data_ids])
        search_params = dict(
            start_date=JsonStringSchema(format='date-time'),
            end_date=JsonStringSchema(format='date-time'),
            bbox=JsonArraySchema(items=(JsonNumberSchema(),
                                        JsonNumberSchema(),
                                        JsonNumberSchema(),
                                        JsonNumberSchema())),
            cci_attrs=JsonObjectSchema(
                properties=dict(
                    ecv=JsonStringSchema(enum=ecvs),
                    frequency=JsonStringSchema(enum=frequencies),
                    institute=JsonStringSchema(enum=_INSTITUTES),
                    processing_level=JsonStringSchema(enum=processing_levels),
                    product_string=JsonStringSchema(enum=product_strings),
                    product_version=JsonStringSchema(enum=product_versions),
                    data_type=JsonStringSchema(enum=data_types),
                    sensor=JsonStringSchema(enum=sensors),
                    platform=JsonStringSchema(enum=platforms)
                ),
                additional_properties=False
            )
        )
        search_schema = JsonObjectSchema(
            properties=dict(**search_params),
            additional_properties=False)
        return search_schema

    def search_data(
            self, data_type: str = None, **search_params
    ) -> Iterator[DatasetDescriptor]:
        search_schema = self.get_search_params_schema()
        search_schema.validate_instance(search_params)
        openers = self._get_openers(data_type=data_type)
        desc_iterators = []
        for opener in openers:
            desc_iterators.extend(opener.search_data(**search_params))
        return desc_iterators

    def get_data_opener_ids(
            self, data_id: str = None, data_type: str = None,
    ) -> Tuple[str, ...]:
        self._assert_valid_data_type(data_type)
        if data_id is not None \
                and not self.has_data(data_id, data_type=data_type):
            raise DataStoreError(f'Data resource {data_id!r}'
                                 f' is not available.')
        if data_type is None:
            return list(self._openers.keys())
        opener_ids = []
        for opener_id, opener in self._openers.items():
            for opener_data_type in opener.get_data_types():
                if opener_data_type.is_super_type_of(data_type):
                    opener_ids.append(opener_id)
                    break
        if len(opener_ids) == 0:
            raise DataStoreError(f'Data resource {data_id!r}'
                                 f' is not available as type {data_type!r}.')
        return tuple(opener_ids)

    def get_open_data_params_schema(
            self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        openers = self._get_openers(opener_id=opener_id)
        if data_id is not None:
            for opener in openers:
                if opener.has_data(data_id):
                    return opener.get_open_data_params_schema(data_id)
            raise ValueError(f"No schema for '{data_id}' with provided opener ids.")
        # TODO find solution for when neither data id nor opener id have been provided
        # Have less specific schema?
        return openers[0].get_open_data_params_schema(data_id)

    def open_data(
            self, data_id: str, opener_id: str = None, **open_params
    ) -> xr.Dataset:
        openers = self._get_openers(opener_id=opener_id)
        for opener in openers:
            if opener.has_data(data_id):
                return opener.open_data(data_id, **open_params)
        raise ValueError(f"Could not open data '{data_id}'. No opener found.")

    ############################################################################
    # Implementation helpers

    @classmethod
    def _is_valid_data_type(cls, data_type: str) -> bool:
        return data_type is None \
               or data_type == "vectordatacube" \
               or DATASET_TYPE.is_super_type_of(data_type) \
               or GEO_DATA_FRAME_TYPE.is_super_type_of(data_type)

    @classmethod
    def _assert_valid_data_type(cls, data_type):
        if not cls._is_valid_data_type(data_type):
            raise DataStoreError(
                f'Data type must be {DATASET_TYPE!r}, {GEO_DATA_FRAME_TYPE!r}, '
                f'or {VECTOR_DATA_CUBE_TYPE!r}, but got {data_type!r}')

    def _assert_valid_opener_id(self, opener_id):
        if opener_id is not None and opener_id not in list(self._openers.keys()):
            raise DataStoreError(
                f'Data opener identifier must be {DATASET_OPENER_ID!r}, '
                f'{DATAFRAME_OPENER_ID!r}, or {VECTORDATACUBE_OPENER_ID!r},'
                f'but got {opener_id!r}'
            )
