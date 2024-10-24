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

CDC_SHORT_DATA_STORE_ID = 'esa-cdc'

DATAFRAME_OPENER_ID = f'dataframe:geojson:{CDC_SHORT_DATA_STORE_ID}'
DATASET_OPENER_ID = f'dataset:zarr:{CDC_SHORT_DATA_STORE_ID}'
VECTORDATACUBE_OPENER_ID = f'vectordatacube::{CDC_SHORT_DATA_STORE_ID}'
DATA_ARRAY_NAME = 'var_data'
OPENSEARCH_CEDA_URL = 'https://archive.opensearch.ceda.ac.uk/opensearch/request'
CCI_ODD_URL = 'https://archive.opensearch.ceda.ac.uk/' \
              'opensearch/description.xml?parentIdentifier=cci'

# to test with opensearch test, use the following two lines
# instead of the upper two
# OPENSEARCH_CEDA_URL = "http://opensearch-test.ceda.ac.uk/opensearch/request"
# CCI_ODD_URL = 'http://opensearch-test.ceda.ac.uk/' \
#               'opensearch/description.xml?parentIdentifier=cci'

DEFAULT_TILE_SIZE = 1000
DEFAULT_RETRY_BACKOFF_MAX = 40  # milliseconds
DEFAULT_RETRY_BACKOFF_BASE = 1.001
DEFAULT_NUM_RETRIES = 200

CCI_MAX_IMAGE_SIZE = 2500

COMMON_SPATIAL_COORD_VAR_NAMES = [
    'lat', 'lon', 'latitude', 'longitude',
    'latitude_centers', 'x', 'y', 'xc', 'yc'
]
COMMON_TIME_COORD_VAR_NAMES = ['time', 't', 'start_time', 'end_time']
COMMON_COORD_VAR_NAMES = COMMON_SPATIAL_COORD_VAR_NAMES + COMMON_TIME_COORD_VAR_NAMES

MONTHS = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY',
          'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
