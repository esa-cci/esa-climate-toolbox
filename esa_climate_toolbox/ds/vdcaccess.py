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
from typing import Dict
from typing import Hashable
from typing import Mapping
from typing import Optional
from typing import Tuple
import dask.array as da
import numpy as np
import xarray
import warnings

from xcube.core.store import DataDescriptor
from xcube.core.store import DataType
from xcube.core.store import DataTypeLike
from xcube.core.store import VariableDescriptor
from xcube.util.assertions import assert_true
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonObjectSchema


class VectorDataCube(xarray.Dataset):
    """A wrapper class around an xarray Dataset to keep it separate."""

VECTOR_DATA_CUBE_TYPE = DataType(
    VectorDataCube,
    ['vectordatacube', 'xarray.Dataset']
)

DataType.register_data_type(VECTOR_DATA_CUBE_TYPE)


def _attrs_to_json(attrs: Mapping[Hashable, Any]) \
        -> Optional[Dict[str, Any]]:
    new_attrs: Dict[str, Any] = {}
    for k, v in attrs.items():
        if isinstance(v, np.ndarray):
            v = v.tolist()
        elif isinstance(v, da.Array):
            v = np.array(v).tolist()
        if isinstance(v, float) and np.isnan(v):
            v = None
        new_attrs[str(k)] = v
    return new_attrs


class VectorDataCubeDescriptor(DataDescriptor):
    """
    A descriptor for an N-dimensional vector data cube whose spatial properties are
    designated by a geometry cooordinate. The cube is represented by an xarray.Dataset.
    Comprises a description of the data variables contained in the dataset.

    Regarding *time_range* and *time_period* parameters, please refer to
    https://github.com/dcs4cop/xcube/blob/master/docs/source/storeconv.md#date-time-and-duration-specifications

    :param data_id: An identifier for the data
    :param data_type: The data type of the data described
    :param crs: A coordinate reference system identifier,
        as an EPSG, PROJ or WKT string
    :param bbox: A bounding box of the data
    :param time_range: Start and end time delimiting this data's
        temporal extent
    :param time_period: The data's periodicity
        if it is evenly temporally resolved
    :param dims: A mapping of the dataset's dimensions to their sizes
    :param coords: mapping of the dataset's data coordinate names
        to instances of :class:VariableDescriptor
    :param data_vars: A mapping of the dataset's variable names
        to instances of :class:VariableDescriptor
    :param attrs: A mapping containing arbitrary attributes of the dataset
    :param open_params_schema: A JSON schema describing the parameters
        that may be used to open this data
    """

    def __init__(self,
                 data_id: str,
                 *,
                 data_type: DataTypeLike = VECTOR_DATA_CUBE_TYPE,
                 crs: str = None,
                 bbox: Tuple[float, float, float, float] = None,
                 time_range: Tuple[Optional[str], Optional[str]] = None,
                 time_period: str = None,
                 spatial_res: float = None,
                 dims: Mapping[str, int] = None,
                 coords: Mapping[str, 'VariableDescriptor'] = None,
                 data_vars: Mapping[str, 'VariableDescriptor'] = None,
                 attrs: Mapping[Hashable, any] = None,
                 open_params_schema: JsonObjectSchema = None,
                 **additional_properties):
        super().__init__(data_id=data_id,
                         data_type=data_type,
                         crs=crs,
                         bbox=bbox,
                         time_range=time_range,
                         time_period=time_period,
                         open_params_schema=open_params_schema)
        assert_true(VECTOR_DATA_CUBE_TYPE.is_super_type_of(data_type),
                    f'illegal data_type,'
                    f' must be compatible with {VECTOR_DATA_CUBE_TYPE!r}')
        if additional_properties:
            warnings.warn(f'Additional properties received;'
                          f' will be ignored: {additional_properties}')
        self.dims = dict(dims) if dims else None
        self.spatial_res = spatial_res
        self.coords = coords if coords else None
        self.data_vars = data_vars if data_vars else None
        self.attrs = _attrs_to_json(attrs) if attrs else None

    @classmethod
    def get_schema(cls) -> JsonObjectSchema:
        schema = super().get_schema()
        schema.properties.update(
            dims=JsonObjectSchema(
                additional_properties=JsonIntegerSchema(minimum=0)
            ),
            coords=JsonObjectSchema(
                additional_properties=VariableDescriptor.get_schema()
            ),
            data_vars=JsonObjectSchema(
                additional_properties=VariableDescriptor.get_schema()
            ),
            attrs=JsonObjectSchema(
                additional_properties=True
            ),
        )
        schema.required = ['data_id', 'data_type']
        schema.additional_properties = False
        schema.factory = cls
        return schema
