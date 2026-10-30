# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative
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

"""Shared implementation of DataTree reductions and their output conversions."""

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import xarray as xr
from shapely.geometry import Point

from esa_climate_toolbox.core.types import DimNamesLike, VarNamesLike, ValidationError


def _dimensions(variable, dim):
    return list(variable.dims) if dim is None else [d for d in dim if d in variable.dims]


def _weights(variable, dim, spatial_weighting):
    if spatial_weighting and 'lat' in dim:
        if 'lat' not in variable.coords or variable.lat.dims != ('lat',):
            raise ValidationError("Spatial weighting requires a one-dimensional 'lat' coordinate.")
        weights = np.cos(np.deg2rad(variable.lat))
        if not np.isfinite(weights).all() or (abs(variable.lat) > 90).any():
            raise ValidationError('Spatial weighting requires finite latitudes within [-90, 90].')
        return weights
    return None


def _reduce_variable(variable, dim, method, spatial_weighting):
    if method == 'count':
        return variable.notnull().sum(dim=dim)
    weights = (_weights(variable, dim, spatial_weighting)
               if method in ('mean', 'sum', 'std') else None)
    if weights is not None and method in ('mean', 'sum', 'std'):
        return getattr(variable.weighted(weights), method)(dim=dim, keep_attrs=True)
    # Dask's exact median requires each reduction axis to occupy one chunk.
    if method == 'median' and variable.chunks is not None:
        variable = variable.chunk({d: -1 for d in dim})
    if method == 'median':
        return variable.reduce(_nanmedian, dim=dim, keep_attrs=True)
    return getattr(variable, method)(dim=dim, skipna=True, keep_attrs=True)


def _nanmedian(data, axis=None):
    # Xarray passes axis=None when all axes are reduced; Dask requires explicit axes.
    return np.nanmedian(data, axis=tuple(range(data.ndim)) if axis is None else axis)


def _combine(parts, method):
    """Align only the remaining coordinates, never the original spatial grids."""
    axis = '_aggregate_node'
    while any(axis in part.dims or axis in part.coords for part in parts):
        axis += '_'
    combined = xr.concat(parts, dim=axis, join='outer', coords='minimal', compat='override')
    return getattr(combined, method)(dim=axis, skipna=True)


def _pool_variable(variables, dim, methods, spatial_weighting):
    dims = [_dimensions(variable, dim) for variable in variables]
    remaining = [set(variable.dims) - set(ds_dim)
                 for variable, ds_dim in zip(variables, dims)]
    if any({'lat', 'lon'} & rest for rest in remaining):
        raise ValidationError('Aggregation across nodes requires reducing both spatial dimensions lat/lon.')
    if any(rest != remaining[0] for rest in remaining[1:]):
        raise ValidationError('Variables must have matching remaining dimensions across nodes.')

    results = {}
    if any(method in methods for method in ('mean', 'sum', 'std')):
        totals, masses, means, moments = [], [], [], []
        for variable, ds_dim in zip(variables, dims):
            weight = _weights(variable, ds_dim, spatial_weighting)
            weight = xr.ones_like(variable) if weight is None else weight.broadcast_like(variable)
            weight = weight.where(variable.notnull(), 0)
            mass = weight.sum(dim=ds_dim)
            total = (variable * weight).sum(dim=ds_dim, skipna=True)
            mean = total / mass.where(mass != 0)
            masses.append(mass)
            totals.append(total)
            means.append(mean)
            if 'std' in methods:
                moments.append((weight * (variable - mean) ** 2).sum(dim=ds_dim, skipna=True))
        total_mass = _combine(masses, 'sum')
        total = _combine(totals, 'sum')
        mean = total / total_mass.where(total_mass != 0)
        results.update(mean=mean, sum=total)
        if 'std' in methods:
            # Parallel variance formula includes variation between node means.
            moment = _combine([
                m2 + mass * (local_mean - mean) ** 2
                for m2, mass, local_mean in zip(moments, masses, means)
            ], 'sum')
            results['std'] = np.sqrt(moment / total_mass.where(total_mass != 0))

    for method in methods:
        if method in results:
            continue
        if method == 'median':
            axis = '_aggregate_sample'
            while any(axis in variable.dims or axis in variable.coords for variable in variables):
                axis += '_'
            samples = []
            for variable, ds_dim in zip(variables, dims):
                sample = variable.reset_coords(drop=True)
                sample = sample.drop_vars([d for d in ds_dim if d in sample.coords])
                # Avoid constructing a potentially huge spatial/time MultiIndex.
                sample = (sample.stack({axis: ds_dim}, create_index=False)
                          if ds_dim else sample.expand_dims(axis))
                samples.append(sample)
            pooled = xr.concat(samples, dim=axis, join='outer')
            if pooled.chunks is not None:
                pooled = pooled.chunk({axis: -1})
            results[method] = pooled.reduce(_nanmedian, dim=axis)
        else:
            parts = [_reduce_variable(variable, ds_dim, method, False)
                     for variable, ds_dim in zip(variables, dims)]
            results[method] = _combine(parts, 'sum' if method == 'count' else method)
    return {method: results[method].assign_attrs(variables[0].attrs) for method in methods}


def _vector_regions(vector_data, vector_path, node_column, paths):
    if vector_data is not None and vector_path is not None:
        raise ValidationError('Supply either vector_data or vector_path, not both.')
    if vector_path is not None:
        vector_data = gpd.read_file(vector_path)
    if vector_data is None:
        return None
    if vector_data.crs is None:
        raise ValidationError('Vector data must declare a CRS.')
    if node_column not in vector_data:
        raise ValidationError(f'Vector data has no node mapping column {node_column!r}.')
    regions = vector_data.set_index(node_column)
    for path in paths:
        if path not in regions.index or (regions.index == path).sum() != 1:
            raise ValidationError(f'Expected exactly one geometry mapping for {path!r}.')
        geometry = regions.geometry.loc[path]
        if (geometry is None or geometry.is_empty or not geometry.is_valid
                or geometry.geom_type not in ('Polygon', 'MultiPolygon')):
            raise ValidationError(f'Expected a valid polygon geometry for {path!r}.')
    return regions.loc[paths]


def _masked_dataset(ds, geometry):
    if any(dim not in ds.coords or ds[dim].dims != (dim,) for dim in ('lat', 'lon')):
        raise ValidationError('Vector masking requires one-dimensional lat/lon coordinates.')
    lon, lat = np.meshgrid(ds.lon.values, ds.lat.values)
    mask = xr.DataArray(shapely.intersects_xy(geometry, lon, lat),
                        dims=('lat', 'lon'), coords={'lat': ds.lat, 'lon': ds.lon})
    result = ds.copy()
    for name, variable in ds.data_vars.items():
        if {'lat', 'lon'} <= set(variable.dims):
            result[name] = variable.where(mask)
    return result


def _centroid(ds, names):
    valid = None
    for name in names:
        variable = ds[name]
        if not {'lat', 'lon'} <= set(variable.dims):
            continue
        other = [dim for dim in variable.dims if dim not in ('lat', 'lon')]
        mask = variable.notnull().any(dim=other).transpose('lat', 'lon')
        valid = mask if valid is None else valid | mask
    if valid is None:
        raise ValidationError('Cannot infer geometry: selected variables have no lat/lon dimensions.')
    valid = valid.values
    if not valid.any():
        raise ValidationError('Cannot infer geometry: selected variables have no valid pixels.')
    lon, lat = np.meshgrid(ds.lon.values, ds.lat.values)
    lon, lat = np.deg2rad(lon[valid]), np.deg2rad(lat[valid])
    # Mean unit vectors provide a centroid across the antimeridian as well.
    x, y = np.mean(np.cos(lat) * np.cos(lon)), np.mean(np.cos(lat) * np.sin(lon))
    z = np.mean(np.sin(lat))
    if np.linalg.norm([x, y, z]) < 1e-12:
        raise ValidationError('Valid pixels have no unique spherical centroid.')
    return Point(float(np.rad2deg(np.arctan2(y, x))),
                 float(np.rad2deg(np.arctan2(z, np.hypot(x, y)))))


def aggregate_datatree(tree, var, dim, methods, spatial_weighting, nodes,
                       aggregate_nodes, output_format, vector_data, vector_path,
                       node_column, monitor, statistics=False):
    if output_format not in ('datatree', 'vector_data_cube', 'dataframe'):
        raise ValidationError(f'Unknown output_format {output_format!r}.')
    if not methods or set(methods) - {'mean', 'sum', 'min', 'max', 'median', 'count', 'std'}:
        raise ValidationError('Select at least one supported aggregation method.')
    if aggregate_nodes and output_format != 'datatree':
        raise ValidationError('Per-node output formats require aggregate_nodes=False.')
    leaves = {node.path: node.to_dataset() for node in tree.leaves if node.has_data}
    paths = list(leaves) if nodes is None else list(nodes)
    if any(not isinstance(path, str) for path in paths):
        raise ValidationError('nodes must contain dataset leaf names or paths.')
    paths = [path if path.startswith('/') else '/' + path for path in paths]
    if not paths or len(paths) != len(set(paths)) or any(path not in leaves for path in paths):
        raise ValidationError('nodes must select distinct, non-empty dataset leaves by name or path.')
    names = (VarNamesLike.convert(var) if var else list(dict.fromkeys(
        name for path in paths for name in leaves[path].data_vars
    )))
    dims = DimNamesLike.convert(dim) if dim is not None else None
    for path in paths:
        for name in names:
            if name not in leaves[path]:
                raise ValidationError(f'Selected variable {name!r} is missing from {path!r}.')
            if not np.issubdtype(leaves[path][name].dtype, np.number):
                raise ValidationError(f'Selected variable {name!r} must be numeric.')
    if dims is not None:
        available = set().union(*(set(leaves[path][name].dims) for path in paths for name in names))
        if set(dims) - available:
            raise ValidationError(f'Unknown dimensions: {sorted(set(dims) - available)}.')
    regions = _vector_regions(vector_data, vector_path, node_column, paths)
    geometry_output = output_format != 'datatree'

    selected, result = {}, {}
    with monitor.starting('Aggregate DataTree', total_work=len(paths) + 1):
        masking = regions.to_crs('EPSG:4326') if regions is not None else None
        for path in paths:
            selected[path] = (_masked_dataset(leaves[path], masking.geometry.loc[path])
                              if masking is not None else leaves[path])
            monitor.progress(1)
        if aggregate_nodes:
            variables = {}
            for name in names:
                pooled = _pool_variable([selected[path][name] for path in paths],
                                        dims, methods, spatial_weighting)
                for method, value in pooled.items():
                    variables[f'{name}_{method}' if statistics else name] = value
            output = xr.DataTree(xr.Dataset(variables, attrs=tree.attrs))
        else:
            for path, ds in selected.items():
                local = ds.copy()
                converted = {}
                for name in names:
                    ds_dim = _dimensions(ds[name], dims)
                    for method in methods:
                        output_name = f'{name}_{method}' if statistics else name
                        value = _reduce_variable(ds[name], ds_dim, method, spatial_weighting)
                        local[output_name] = value
                        converted[output_name] = value
                result[path] = xr.Dataset(converted)
                selected[path] = local
            if not geometry_output:
                output = tree.copy()
                if nodes is not None:
                    keep = {'/'}
                    for path in paths:
                        components = path.strip('/').split('/')
                        keep.update('/' + '/'.join(components[:i])
                                    for i in range(1, len(components) + 1))
                    for node in reversed(tuple(output.subtree)):
                        if node.path not in keep:
                            node.orphan()
                for path in paths:
                    output[path].dataset = selected[path]
            else:
                output = _convert_output(result, leaves, regions, names, output_format)
        monitor.progress(1)
    return output


def _convert_output(results, source, regions, names, output_format):
    for ds in results.values():
        for variable in ds.data_vars.values():
            if output_format == 'dataframe' and variable.ndim:
                raise ValidationError('dataframe output requires scalar variables with no remaining dimensions.')
            if output_format == 'vector_data_cube' and variable.dims != ('time',):
                raise ValidationError('vector_data_cube output requires only the time dimension to remain.')
    paths = list(results)
    geometries = ([regions.geometry.loc[path] for path in paths] if regions is not None
                  else [_centroid(source[path], names) for path in paths])
    crs = regions.crs if regions is not None else 'EPSG:4326'
    if output_format == 'dataframe':
        rows = [{name: variable.item() if variable.chunks is None else variable.compute().item()
                 for name, variable in ds.data_vars.items()} for ds in results.values()]
        return gpd.GeoDataFrame(rows, index=pd.Index(paths, name='path'),
                                geometry=geometries, crs=crs)
    output = xr.concat(list(results.values()), dim=pd.Index(paths, name='feature'),
                       join='outer', coords='minimal', compat='override')
    output = output.assign_coords(geometry=('feature', np.asarray(geometries, dtype=object)))
    # Match the geometry-index convention used by the CCI vector data cube opener.
    import xvec
    return output.set_xindex('geometry', xvec.GeometryIndex, crs=crs)
