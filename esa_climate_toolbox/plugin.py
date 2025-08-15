# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
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

from xcube.constants import EXTENSION_POINT_DATA_OPENERS, EXTENSION_POINT_DATA_WRITERS
from xcube.core.store.fs.registry import register_fs_data_accessor_class
from xcube.util import extension

from esa_climate_toolbox.ds.geodataframe import GeoDataFrameKmlFsDataAccessor

_FS_STORAGE_ITEMS = (
    ("abfs", "Azure blob compatible object storage"),
    ("file", "local filesystem"),
    ("ftp", "FTP filesystem"),
    ("https", "HTTPS filesystem"),
    ("memory", "in-memory filesystem"),
    ("reference", "reference filesystem"),
    ("s3", "AWS S3 compatible object storage"),
)


def init_plugin(ext_registry: extension.ExtensionRegistry):
    factory = "xcube.core.store.fs.registry:get_fs_data_accessor_class"

    register_fs_data_accessor_class(GeoDataFrameKmlFsDataAccessor)

    def _add_fs_data_accessor_ext(
        point: str,
        ext_type: str,
        protocol: str,
        data_type: str,
        format_id: str,
        description: str,
        file_extensions: list[str],
    ):
        factory_args = (protocol, data_type, format_id)
        loader = extension.import_component(factory, call_args=factory_args)
        ext_registry.add_extension(
            point=point,
            loader=loader,
            name=f"{data_type}:{format_id}:{protocol}",
            description=f"Data {ext_type} for a {description} in {storage_description}",
            extensions=file_extensions,
        )

    for protocol, storage_description in _FS_STORAGE_ITEMS:
        _add_fs_data_accessor_ext(
            EXTENSION_POINT_DATA_OPENERS,
            "opener",
            protocol,
            "geodataframe",
            "kml",
            "gpd.GeoDataFrame in KML format",
            [".kml"],
        )
        _add_fs_data_accessor_ext(
            EXTENSION_POINT_DATA_WRITERS,
            "writer",
            protocol,
            "geodataframe",
            "kml",
            "gpd.GeoDataFrame in KML format",
            [".kml"],
        )
