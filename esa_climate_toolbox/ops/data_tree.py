# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative

"""Operations for creating hierarchical xarray DataTrees."""

from collections.abc import Sequence
from pathlib import Path
from typing import Optional

import importlib
import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely.geometry import box

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.opimpl import subset_spatial_impl
from esa_climate_toolbox.core.types import DatasetLike, PolygonLike
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.util.monitor import Monitor

@op(tags=['geometric', 'spatial', 'subset', 'datatree'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('identifier_column', data_type=str)
@op_input('vector_data', data_type=gpd.GeoDataFrame, nullable=True)
@op_input('vector_path', data_type=str, nullable=True)
@op_input('features', nullable=True)
@op_input('region', data_type=PolygonLike, nullable=True)
@op_input('fallback_column', data_type=str, nullable=True)
@op_input('group_columns', nullable=True)
@op_return()
def dataset_to_datatree(
        ds: DatasetLike.TYPE,
        identifier_column: str,
        vector_data: Optional[gpd.GeoDataFrame] = None,
        vector_path: Optional[str] = None,
        features: Optional[Sequence[str]] = None,
        region: PolygonLike.TYPE = None,
        fallback_column: str = None,
        group_columns: Optional[Sequence[str]] = None,
        monitor: Monitor = Monitor.NONE,
) -> xr.DataTree:
    """Split a dataset into a DataTree based on bounding boxes of vector data.

    The vector data must be provided either from  ``regions`` or ``vector_path``,
    exactly one of them must be supplied. Each vector feature produces one dataset node.
    Its geometry's bounding box is used for the spatial subset. The node name is
    read from ``identifier_column`` or the optional ``fallback_column``.
    Optional ``group_columns`` form parent groups in the given order.

    The input dataset must use one-dimensional ``lon`` and ``lat`` coordinates,
    consider using the ``normalize`` operation first.
    Vector geometries are transformed to WGS 84, which is the coordinate
    reference system expected by these coordinates. A vector source without a
    declared CRS is rejected to avoid silently producing incorrect subsets.

    The operation will only create leaves for regions whose bounding boxes at least
    overlap with the dataset. Further restrictions can be made using the parameters
    ``features`` to select a subset of features from the dataframe and ``region``
    to only consider features in a spatial subset.

    :param ds: Dataset to split into regional subsets.
    :param identifier_column: Column containing the unique leaf node names.
    :param vector_data: GeoDataFrame containing polygon geometries.
    :param vector_path: Path or URL readable by ``geopandas.read_file``.
    :param features: If provided, only these features are considered when
        creating the tree nodes.
    :param region: If provided, only features intersecting this region will be
        considered. May be given as one of the following:
        1. a shapely.geometry.shapely.geometry.Polygon object
        2. a string “min_lon, min_lat, max_lon, max_lat”
        3. a WKT string “POLYGON ((RING))” or ”POLYGON ((OUTER-RING), (INNER-RING), …)”
        4. a list of coordinates [(lon, lat), (lon, lat), (lon, lat)]
        5. a list or tuple [min_lon, min_lat, max_lon, max_lat]
    :param fallback_column: Column from which to take leaf node names
        in case identifier_column is empty.
    :param group_columns: Columns forming optional parent groups.
    :param monitor: A progress monitor.
    :return: A DataTree with an empty root and one dataset per region.
    """
    ds = DatasetLike.convert(ds)
    vector_data = _read_regions(vector_data, vector_path)
    group_columns = list(group_columns or [])
    _validate_columns(vector_data, identifier_column, fallback_column, group_columns)

    vector_data[identifier_column] = (
        vector_data[identifier_column].replace("None", pd.NA).
        fillna(vector_data[fallback_column])) \
        if fallback_column else vector_data[identifier_column]

    vector_data = vector_data[vector_data[identifier_column].isin(features)] \
        if features else vector_data

    vector_data = vector_data[vector_data.geometry.intersects(PolygonLike.convert(region))] \
        if region else vector_data

    if vector_data.crs is None:
        raise ValidationError(
            'The regions must define a coordinate reference system.'
        )
    vector_data = vector_data.to_crs('EPSG:4326')

    datasets = {}
    with monitor.starting('dataset_to_datatree', total_work=len(vector_data)):
        for row_index, row in vector_data.iterrows():
            geometry = row.geometry
            if geometry is None or geometry.is_empty:
                raise ValidationError(
                    f'Region at index {row_index!r} has no geometry.'
                )
            if geometry.geom_type not in ('Polygon', 'MultiPolygon'):
                raise ValidationError(
                    f'Region at index {row_index!r} must be a polygon, '
                    f'but is {geometry.geom_type!r}.'
                )

            components = [
                _path_component(row[column], column, row_index)
                for column in group_columns
            ]
            components.append(
                _path_component(
                    row[identifier_column], identifier_column, row_index
                )
            )
            path = '/' + '/'.join(components)
            if path in datasets:
                raise ValidationError(
                    f'Multiple regions produce the DataTree path {path!r}.'
                )

            min_x, min_y, max_x, max_y = geometry.bounds
            bbox = box(min_x, min_y, max_x, max_y)
            try:
                datasets[path] = subset_spatial_impl(
                    ds, bbox, mask=False, monitor=monitor.child(1)
                )
            except ValueError as error:
                if str(error) == "Can not select a region outside dataset boundaries.":
                    pass

    return xr.DataTree.from_dict(datasets)


@op(tags=['geometric', 'spatial', 'datatree', 'LAKES'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('lakes', nullable=True)
@op_input('region', data_type=PolygonLike, nullable=True)
@op_input('group_into_continents',  data_type=bool, default_value=True)
@op_return()
def dataset_to_lakes_datatree(
        ds: DatasetLike.TYPE,
        lakes: Optional[Sequence[str]] = None,
        region: PolygonLike.TYPE = None,
        group_into_continents: bool = True,
        monitor: Monitor = Monitor.NONE
) -> xr.DataTree:
    """Split a dataset into subsets stored in a DataTree. The subsets correspond
    to geometries of lakes as defined in the LAKES CCI project. This operation
    has been designed to work with datasets of the LAKES ECV, however, it
    works with any dataset with one-dimensional ``lon`` and ``lat`` coordinates.

    Restrictions can be made using the parameters ``lakes`` to select a subset of
    lakes from the dataframe and ``region`` to only consider lakes in a spatial region.

    :param ds: Dataset to split into regional subsets.
    :param lakes: If provided, only these lakes are considered when
        creating the tree nodes.
    :param region: If provided, only lakes intersecting this region will be
        considered. May be given as one of the following:
        1. a shapely.geometry.shapely.geometry.Polygon object
        2. a string “min_lon, min_lat, max_lon, max_lat”
        3. a WKT string “POLYGON ((RING))” or ”POLYGON ((OUTER-RING), (INNER-RING), …)”
        4. a list of coordinates [(lon, lat), (lon, lat), (lon, lat)]
        5. a list or tuple [min_lon, min_lat, max_lon, max_lat]
    :param group_into_continents: Switch to add a group in the datatree by continents.
        True by default.
    :param monitor: A progress monitor.
    :return: A DataTree with an empty root and one dataset per region.
    """
    with importlib.resources.path(
            "esa_climate_toolbox.ops.data.lakes", "lakes_simplified_v2.shp"
    ) as p:
        return dataset_to_datatree(
            ds,
            identifier_column="name",
            vector_path=str(p),
            features=lakes,
            region=region,
            fallback_column="short_name",
            group_columns=["continent"] if group_into_continents else None,
            monitor=monitor
        )


def _read_regions(
        regions: Optional[gpd.GeoDataFrame],
        vector_path: Optional[str],
) -> gpd.GeoDataFrame:
    if (regions is None) == (vector_path is None):
        raise ValidationError(
            'Exactly one of regions and vector_path must be provided.'
        )
    if vector_path is not None:
        if not isinstance(vector_path, (str, Path)):
            raise ValidationError('vector_path must be a path or URL.')
        regions = gpd.read_file(vector_path)
    elif hasattr(regions, 'lazy_data_frame'):
        regions = regions.lazy_data_frame
    if not isinstance(regions, gpd.GeoDataFrame):
        raise ValidationError('regions must be a GeoDataFrame.')
    return regions


def _validate_columns(
        regions: gpd.GeoDataFrame,
        identifier_column: str,
        fallback_column: str,
        group_columns: Sequence[str],
):
    columns = [identifier_column, fallback_column, *group_columns] if fallback_column \
        else [identifier_column, *group_columns]
    missing = [column for column in columns if column not in regions.columns]
    if missing:
        raise ValidationError(
            f'Specified columns do not exist: {", ".join(missing)}.'
        )
    if len(columns) != len(set(columns)):
        raise ValidationError(
            'identifier_column, fallback_column and group_columns must be distinct.'
        )


def _path_component(value, column: str, row_index) -> str:
    if pd.isna(value):
        raise ValidationError(
            f'Column {column!r} is empty at index {row_index!r}.'
        )
    component = str(value).strip()
    if not component or component in ('.', '..') or '/' in component:
        raise ValidationError(
            f'Column {column!r} contains invalid DataTree node name '
            f'{component!r} at index {row_index!r}.'
        )
    return component
