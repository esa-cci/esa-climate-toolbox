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
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from .constants import COMMON_COORD_VAR_NAMES


def normalize_dims_description(dims: dict) -> dict:
    new_dims = dims.copy()
    if 'latitude' in new_dims:
        new_dims['lat'] = new_dims.pop('latitude')
    if 'longitude' in new_dims:
        new_dims['lon'] = new_dims.pop('longitude')
    if 'latitude_centers' in new_dims:
        new_dims['lat'] = new_dims.pop('latitude_centers')
        new_dims['lon'] = new_dims['lat'] * 2
    return new_dims


def normalize_variable_dims_description(
        var_dims: List[str]
) -> Optional[List[str]]:
    if ('lat' in var_dims and 'lon' in var_dims) or \
            ('latitude' in var_dims and 'longitude' in var_dims) or \
            ('latitude_centers' in var_dims) or \
            ('x' in var_dims and 'y' in var_dims) or \
            ('xc' in var_dims and 'yc' in var_dims):
        if 'y' in var_dims:
            y_dim_name = 'y'
        elif 'yc' in var_dims:
            y_dim_name = 'yc'
        else:
            y_dim_name = 'lat'
        if 'x' in var_dims:
            x_dim_name = 'x'
        elif 'xc' in var_dims:
            x_dim_name = 'xc'
        else:
            x_dim_name = 'lon'
        if var_dims != ['time', y_dim_name, x_dim_name]:
            other_dims = []
            for dim in var_dims:
                if dim not in COMMON_COORD_VAR_NAMES:
                    other_dims.append(dim)
            new_dims = ['time', y_dim_name, x_dim_name]
            for i in range(len(other_dims)):
                new_dims.insert(i + 1, other_dims[i])
            var_dims = new_dims
    return var_dims


def normalize_var_infos(var_infos: Dict[str, Dict[str, Any]]) \
        -> Dict[str, Dict[str, Any]]:
    if 'latitude_centers' in var_infos:
        var_info = var_infos.pop('latitude_centers')
        var_infos['lat'] = dict(data_type=var_info['data_type'],
                                dimensions='lat',
                                units=var_info.get('units'),
                                long_name='latitude',
                                size=var_info.get('size'),
                                shape=var_info.get('shape'),
                                chunk_sizes=var_info.get('chunk_sizes'))
        var_infos['lon'] = dict(data_type=var_info['data_type'],
                                dimensions='lon',
                                units=var_info.get('units'),
                                long_name='longitude',
                                size=var_info.get('size') * 2,
                                shape=[item * 2 for item in
                                       var_info.get('shape', [])],
                                chunk_sizes=[item * 2 for item in var_info.get(
                                    'shape', [])]
                                )
    return var_infos


def normalize_coord_names(coord_names: List[str]) -> List[str]:
    if 'latitude_centers' in coord_names:
        coord_names.remove('latitude_centers')
        coord_names.append('lat')
        coord_names.append('lon')
    return coord_names
