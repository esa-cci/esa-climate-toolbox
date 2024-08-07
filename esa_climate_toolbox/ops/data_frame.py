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

import dask.array as da
import functools
import geopandas as gpd
import math
import pandas as pd
import pyproj
import shapely
import shapely.ops
import xarray as xr
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
import warnings

from xcube.core.extract import get_cube_values_for_points
from xcube.core.gridmapping import GridMapping

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.types import DataFrameLike
from esa_climate_toolbox.core.types import DatasetLike
from esa_climate_toolbox.core.types import GeometryLike
from esa_climate_toolbox.core.types import GeoDataFrame
from esa_climate_toolbox.core.types import Literal
from esa_climate_toolbox.core.types import PolygonLike
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.core.types import VarName
from esa_climate_toolbox.core.types import VarNamesLike
from esa_climate_toolbox.util.monitor import Monitor


_DEG2RAD = math.pi / 180.
_REGION_MODES = [
    'almost_equals',
    'contains',
    'crosses',
    'disjoint',
    'intersects',
    'touches',
    'within',
]

ReprojectionFunc = Callable[[float, float], Tuple[float, float]]


@op(tags=['statistics'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var_names', value_set_source='df', data_type=VarNamesLike)
@op_input('aggregate_geometry', data_type=bool)
def aggregate_statistics(df: DataFrameLike.TYPE,
                         var_names: VarNamesLike.TYPE = None,
                         aggregate_geometry: bool = False,
                         monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Aggregate columns into count, mean, median, sum, std, min, and max. Return a
    new (Geo)DataFrame with a single row containing all aggregated values.
    Specify whether the geometries of the GeoDataFrame are to be aggregated.
    All geometries are merged union-like.

    The return data type will always be the same as the input data type.

    :param df: The (Geo)DataFrame to be analysed
    :param var_names: Variables to be aggregated ('None' uses all aggregatable columns)
    :param aggregate_geometry: Aggregate (union like) the geometry and add it
        to the resulting GeoDataFrame
    :param monitor: Monitor for progress bar
    :return: returns either DataFrame or GeoDataFrame. Keeps input data type
    """
    vns = VarNamesLike.convert(var_names)

    df_is_geo = isinstance(df, gpd.GeoDataFrame)
    aggregations = ["count", "mean", "median", "sum", "std", "min", "max"]

    # Check var names integrity (aggregatable, exists in data frame)
    types_accepted_for_agg = ['float64', 'int64', 'bool']
    agg_columns = list(df.select_dtypes(include=types_accepted_for_agg).columns)

    if df_is_geo:
        try:
            df['geometry']
        except KeyError as e:
            raise ValidationError('Variable geometry not in GEO data frame!') from e
        agg_columns.append('geometry')

    columns = list(df.columns)

    if vns is None:
        vns = agg_columns

    diff = list(set(vns) - set(columns))
    if len(diff) > 0:
        raise ValidationError('Variable ' + ','.join(diff) + ' not in data frame!')

    diff = list(set(vns) - set(agg_columns))
    if len(diff) > 0:
        raise ValidationError('Variable(s) ' + ','.join(diff) + ' not aggregatable!')

    # Aggregate columns
    if vns is None:
        df_buff = df.select_dtypes(include=types_accepted_for_agg).agg(aggregations)
    else:
        df_buff = df[vns].select_dtypes(
            include=types_accepted_for_agg
        ).agg(aggregations)

    res = {}
    for n in df_buff.columns:
        for a in aggregations:
            val = df_buff[n][a]
            h = n + '_' + a
            res[h] = [val]

    df_agg = pd.DataFrame(res)

    # Aggregate (union) geometry if GeoDataFrame
    if df_is_geo and aggregate_geometry:
        total_work = 100
        num_work_rows = 1 + len(df) // total_work
        with monitor.starting('Aggregating geometry: ', total_work):
            multi_polygon = shapely.geometry.MultiPolygon()
            i = 0
            for rec in df.geometry:
                if monitor.is_cancelled():
                    break
                # noinspection PyBroadException
                try:
                    multi_polygon = multi_polygon.union(other=rec)
                except Exception:
                    pass

                if i % num_work_rows == 0:
                    monitor.progress(work=1)
                i += 1

        df_agg = gpd.GeoDataFrame(df_agg, geometry=[multi_polygon], crs=df.crs)

    return df_agg


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_min(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is
    minimal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmin()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_max(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is
    maximal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmax()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('query_expr')
def query(df: DataFrameLike.TYPE, query_expr: str) -> pd.DataFrame:
    """
    Select records from the given data frame where the given conditional query
    expression evaluates to "True".

    If the data frame *df* contains a geometry column (a ``GeoDataFrame`` object),
    then the query expression *query_expr* can also contain geometric relationship
    tests, for example the expression
    ``"population > 100000 and @within('-10, 34, 20, 60')"`` could be used on a data
    frame with the *population* and a *geometry* column to query for larger cities in
    West-Europe.

    The geometric relationship tests are
    * ``@almost_equals(geom)`` - does a feature's geometry almost equal
    the given ``geom``;
    * ``@contains(geom)`` - does a feature's geometry contain the given ``geom``;
    * ``@crosses(geom)`` - does a feature's geometry cross the given ``geom``;
    * ``@disjoint(geom)`` - does a feature's geometry not at all intersect
    the given ``geom``;
    * ``@intersects(geom)`` - does a feature's geometry intersect with given ``geom``;
    * ``@touches(geom)`` - does a feature's geometry have a point in common with
    given ``geom`` but does not intersect it;
    * ``@within(geom)`` - is a feature's geometry contained within given ``geom``.

    The *geom* argument may be a point ``"<lon>, <lat>"`` text string,
    a bounding box ``"<lon1>, <lat1>, <lon2>, <lat2>"`` text, or any valid geometry WKT.

    :param df: The data frame or dataset.
    :param query_expr: The conditional query expression.
    :return: A new data frame.
    """
    data_frame = DataFrameLike.convert(df)

    local_dict = dict(from_wkt=GeometryLike.convert)
    if hasattr(data_frame, 'geometry') \
            and isinstance(data_frame.geometry, gpd.GeoSeries):

        source_crs = dict(init='epsg:4326')
        try:
            target_crs = data_frame.crs.to_dict() or source_crs
        except AttributeError:
            target_crs = source_crs
        reprojection_func = _get_reprojection_func(source_crs, target_crs)

        def _almost_equals(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.geom_almost_equals,
                                           geometry,
                                           reprojection_func)

        def _contains(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.contains,
                                           geometry,
                                           reprojection_func)

        def _crosses(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.crosses,
                                           geometry,
                                           reprojection_func)

        def _disjoint(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.disjoint,
                                           geometry,
                                           reprojection_func)

        def _intersects(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.intersects,
                                           geometry,
                                           reprojection_func)

        def _touches(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.touches,
                                           geometry,
                                           reprojection_func)

        def _within(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.within,
                                           geometry,
                                           reprojection_func)

        local_dict['almost_equals'] = _almost_equals
        local_dict['contains'] = _contains
        local_dict['crosses'] = _crosses
        local_dict['disjoint'] = _disjoint
        local_dict['intersects'] = _intersects
        local_dict['touches'] = _touches
        local_dict['within'] = _within

    # noinspection PyTypeChecker
    subset_data_frame = data_frame.query(query_expr,
                                         local_dict=local_dict,
                                         global_dict={})

    return _maybe_convert_to_geo_data_frame(data_frame, subset_data_frame)


def _data_frame_geometry_op(instance_method,
                            geometry: GeometryLike,
                            reprojection_func: ReprojectionFunc) -> bool:
    geometry = GeometryLike.convert(geometry)
    if geometry is None:
        return False
    geometry = _transform_coordinates(geometry, reprojection_func)
    if geometry is None:
        return False
    return instance_method(geometry)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
def to_dataset(df: DataFrameLike.TYPE) -> xr.Dataset:
    """
    Convert a dataframe to a dataset.

    :param df: The dataframe.
    :return: A dataset created from the dataframe.
    """
    return DatasetLike.convert(df)


@op(tags=['filter'], version='1.0')
@op_input('ds', data_type=DataFrameLike)
def to_dataframe(ds: DatasetLike.TYPE) -> pd.DataFrame:
    """
    Convert a dataset into a dataframe.

    :param ds: The dataset.
    :return: A dataframe created from the geodataframe.
    """
    return DataFrameLike.convert(ds)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('geometry', data_type=VarName)
@op_input('spatial_coords', data_type=VarNamesLike)
@op_input('crs', data_type=Literal, default_value="EPSG:4362")
def as_geodataframe(
        df: DataFrameLike.TYPE,
        geometry: VarName.TYPE,
        spatial_coords: VarNamesLike.TYPE,
        crs: Literal.TYPE
) -> GeoDataFrame:
    """
    Converts a dataframe into a geodataframe, if possible. The conversion is possible
    if the dataframe contains either a geometry column or columns containing spatial
    coordinates.
    This operation supports specifying which column contains geometries or spatial
    coordinates. If nor value is given, it will be attempted to use a column
    named "geometry"; if this is not given, common name-pair-combinations will be tried.

    :param df: The dataframe.
    :param geometry: The name of the column holding the geometry.
    :param spatial_coords: The names of the columns holding spatial coordinates.
        Longitudinal coordinates come first, Latitudinal second
        Will not be considered when ``geometry`` is given.
    :param crs: The spatial reference system of the geometry.
        Defaults to ``EPSG:4362`` / ``WGS84``.
    :return: A geodataframe created from the dataframe.
    """
    if geometry is not None:
        return gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    elif spatial_coords is not None:
        if not isinstance(spatial_coords, list) or not len(spatial_coords) == 2:
            raise ValidationError(
                f"Invalid parameter 'spatial_coords'. "
                f"Must be list of exactly two names, was '{spatial_coords}'."
            )
        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[spatial_coords[0]], df[spatial_coords[1]]),
            crs=crs
        )
    elif "geometry" in df:
        return gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    combination_sets = [
        ["lon", "lat"],
        ["longitude", "latitude"],
        ["x", "y"]
    ]
    for combination_set in combination_sets:
        if combination_set[0] in df and combination_set[1] in df:
            return gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(combination_set[0], combination_set[1]),
                crs=crs
            )
    raise ValueError("Could not convert dataframe to geodataframe")


@op(tags=['filter'], version='1.0')
@op_input('gdf', data_type=GeoDataFrame)
@op_input('ds', data_type=DatasetLike)
@op_input('var_names', data_type=VarNamesLike)
@op_input('interpolation', value_set=['nearest', 'linear'], default_value='nearest')
@op_input('time_name', data_type=Literal)
@op_input('var_prefix', data_type=Literal)
def add_dataset_values_to_geodataframe(
        gdf: GeoDataFrame,
        ds: DatasetLike.TYPE,
        var_names: VarNamesLike.TYPE,
        interpolation: str = "nearest",
        time_name: Literal = None,
        var_prefix: Literal = None
) -> GeoDataFrame:
    """
    For each geometry in the input geodataframe, the value of the variables
    in the dataset will be determined. If the geometry is not a point,
    its centroid value will be used as point of reference.
    Note that this method also matches time.

    :param gdf: The geodataframe.
    :param ds: The dataset to read variables from.
    :param var_names: Names of variables for which to get values
    :param interpolation: Type of interpolation between dataset pixel centers used
        to derive value for dataframe points.
    :param time_name: Name of time column in the geodataframe. If not given, it is
        either expected to be ``time`` or non-existent.
        :param var_names: Names of variables for which to get values
    :param var_prefix: Prefix the columns of the extracted variables will carry
        in the geodataframe. Default is None.
    :return: The geodataframe, enriched by values from the dataset.
    """
    gdfc = gdf.copy()

    def maybe_convert_geometry(gdf):
        return shapely.centroid(gdf["geometry"])

    gdfc['geometry'] = gdfc.apply(maybe_convert_geometry, axis=1)
    ds_gm = GridMapping.from_dataset(ds)
    ds_xy_names = ds_gm.xy_var_names

    gdfc[ds_xy_names[0]] = gdfc['geometry'].x
    gdfc[ds_xy_names[1]] = gdfc['geometry'].y
    if time_name is not None:
        gdfc['time'] = pd.to_datetime(gdfc[time_name])

    ref_name_pattern = str(var_prefix) + "_{name}" if var_prefix else "{name}"

    cvs = get_cube_values_for_points(
        ds, points=gdf,
        var_names=var_names,
        ref_name_pattern=ref_name_pattern,
        method=interpolation
    )

    for var_name in var_names:
        gdf[var_name] = cvs[var_name]

    return gdf


@op(tags=['filter'], version='1.0')
@op_input('gdf', data_type=DataFrameLike)
@op_input('region_op', data_type=str, value_set=_REGION_MODES)
@op_input('region', data_type=PolygonLike)
@op_input('var_names', value_set_source='gdf', data_type=VarNamesLike)
def data_frame_subset(gdf: gpd.GeoDataFrame,
                      region_op: bool = 'intersects',
                      region: PolygonLike.TYPE = None,
                      var_names: VarNamesLike.TYPE = None) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame subset from given variables (data frame columns)
    and/or region.

    :param gdf: A GeoDataFrame.
    :param region_op: The geometric operation to be performed if *region* is given.
    :param region: A region polygon used to filter rows.
    :param var_names: The variables (columns) to select.
    :return: A GeoDataFrame subset.
    """

    region = PolygonLike.convert(region)

    var_names = VarNamesLike.convert(var_names)

    if not var_names and not region:
        return gdf

    if var_names:
        if 'geometry' not in var_names:
            var_names = ['geometry'] + var_names
        gdf = gdf[var_names]

    if region and region_op:
        geom_str = PolygonLike.format(region)
        gdf = query(gdf, f'@{region_op}("{geom_str}")')

    return gdf


@op(tags=['filter'], version='1.0')
@op_input('gdf', data_type=DataFrameLike)
@op_input('location', data_type=GeometryLike)
@op_input('max_results')
@op_input('max_dist')
@op_input('dist_col_name')
def find_closest(gdf: gpd.GeoDataFrame,
                 location: GeometryLike.TYPE,
                 max_results: int = 1,
                 max_dist: float = 180,
                 dist_col_name: str = 'distance',
                 monitor: Monitor = Monitor.NONE) -> gpd.GeoDataFrame:
    """
    Find the *max_results* records closest to given *location* in the given
    GeoDataFrame *gdf*.
    Return a new GeoDataFrame containing the closest records.

    If *dist_col_name* is given, store the actual distances in this column.

    Distances are great-circle distances measured in degrees from a representative
    center of the given *location* geometry to the representative centres of each
    geometry in the *gdf*.

    :param gdf: The GeoDataFrame.
    :param location: A location given as arbitrary geometry.
    :param max_results: Maximum number of results.
    :param max_dist: Ignore records whose distance is greater than this value in
        degrees.
    :param dist_col_name: Optional name of a new column that will store the actual
        distances.
    :param monitor: A progress monitor.
    :return: A new GeoDataFrame containing the closest records.
    """
    location = GeometryLike.convert(location)
    location_point = location.representative_point()

    target_crs = dict(init='epsg:4326')
    try:
        source_crs = gdf.crs or target_crs
    except AttributeError:
        source_crs = target_crs
    reprojection_func = _get_reprojection_func(source_crs, target_crs)

    try:
        geometries = gdf.geometry
    except AttributeError as e:
        raise ValidationError('Missing default geometry column in data frame.') from e

    num_rows = len(geometries)
    indexes = list()

    # PERF: Note, this operation may be optimized by computing the great-circle
    # distances using numpy array math!

    total_work = 100
    num_work_rows = 1 + num_rows // total_work
    with monitor.starting('Finding closest records', total_work):
        for i in range(num_rows):
            geometry = geometries.iloc[i]
            if geometry is not None:
                # noinspection PyBroadException
                try:
                    representative_point = geometry.representative_point()
                except BaseException:
                    # For some geometries shapely.representative_point() raises
                    # AttributeError or ValueError.
                    # E.g. features that span the poles will raise ValueError.
                    # The quick and dirty solution here is to catch such exceptions
                    # and ignore them.
                    representative_point = None
                if representative_point is not None:
                    representative_point = _transform_coordinates(representative_point,
                                                                  reprojection_func)
                    if representative_point is not None:
                        # noinspection PyTypeChecker
                        dist = great_circle_distance(
                            location_point, representative_point
                        )
                        if dist <= max_dist:
                            indexes.append((i, dist))
            if i % num_work_rows == 0:
                monitor.progress(work=1)

    indexes = sorted(indexes, key=lambda item: item[1])
    num_results = min(max_results, len(indexes))
    indexes, distances = zip(*indexes[0:num_results])

    new_gdf = gdf.iloc[list(indexes)]
    if not isinstance(new_gdf, gpd.GeoDataFrame):
        new_gdf = gpd.GeoDataFrame(new_gdf, crs=source_crs)

    if dist_col_name:
        new_gdf[dist_col_name] = da.array(distances)

    return new_gdf


def great_circle_distance(
        p1: shapely.geometry.Point, p2: shapely.geometry.Point
) -> float:
    """
    Compute great-circle distance on a Sphere in degrees.

    See https://en.wikipedia.org/wiki/Great-circle_distance.

    :param p1: First point (lon, lat) in degrees
    :param p2: Second point (lon, lat) in degrees
    :return: Great-circle distance in degree
    """

    lam1, phi1 = p1.x, p1.y
    lam2, phi2 = p2.x, p2.y
    lambda_diff = abs(lam2 - lam1)

    if lambda_diff > 180.:
        lambda_diff = 360. - lambda_diff

    phi1 *= _DEG2RAD
    phi2 *= _DEG2RAD
    lambda_diff *= _DEG2RAD

    sin_phi1 = math.sin(phi1)
    cos_phi1 = math.cos(phi1)
    sin_phi2 = math.sin(phi2)
    cos_phi2 = math.cos(phi2)
    sin_dlam = math.sin(lambda_diff)
    cos_dlam = math.cos(lambda_diff)

    dx = cos_phi2 * sin_dlam
    dy = cos_phi1 * sin_phi2 - sin_phi1 * cos_phi2 * cos_dlam

    y = math.sqrt(dx * dx + dy * dy)
    x = sin_phi1 * sin_phi2 + cos_phi1 * cos_phi2 * cos_dlam

    return math.atan2(y, x) / _DEG2RAD


# Transforms a geometry that is used in an operator for e.g. feature selection purposes.
# It assures that both use an identical 'crs' (projection)
def _transform_coordinates(geometry: shapely.geometry.base.BaseGeometry,
                           reprojection_func: ReprojectionFunc):
    if reprojection_func is not None:
        # noinspection PyBroadException
        try:
            return shapely.ops.transform(reprojection_func, geometry)
        except BaseException as e:
            warnings.warn(f'coordinate transformation failed: {e}')
            return None
    else:
        return geometry


# Transforms a geometry that is used in an operator for e.g. feature selection purposes.
# It assures that both use an identical 'crs' (projection)
def _get_reprojection_func(source_crs: Dict[str, Any],
                           target_crs: Dict[str, Any]) -> Optional[ReprojectionFunc]:
    if source_crs and target_crs and source_crs != target_crs:
        # noinspection PyBroadException,PyTypeChecker
        return functools.partial(
            pyproj.transform,
            pyproj.Proj(**source_crs),  # source coordinate system
            pyproj.Proj(**target_crs)  # target coordinate system
        )
    else:
        return None


def _maybe_convert_to_geo_data_frame(data_frame: pd.DataFrame,
                                     data_frame_2: pd.DataFrame) -> pd.DataFrame:
    if isinstance(data_frame, gpd.GeoDataFrame) \
            and not isinstance(data_frame_2, gpd.GeoDataFrame):
        return gpd.GeoDataFrame(data_frame_2, crs=data_frame.crs)
    else:
        return data_frame_2
