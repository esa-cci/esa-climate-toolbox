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

import numpy as np

from esa_climate_toolbox.constants import CDC_SHORT_DATA_STORE_ID

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

TIFF_VARS = {
    "CHANGE-CDET": {
        "year_of_change": {
            "band_index": 0,
            "description": "The year of change in the 5-year span of the two years "
                           "from which the LCC product is calulated.",
            "data_type": np.float32,
            "min_value": 1990,
            "max_value": 2019,
            "fill_value": np.nan
        },
        "probability_of_change": {
            "band_index": 1,
            "description": "Probability of the change as returned by the change "
                           "detection algorithm. Original probability values in the "
                           "range [0,1] are mapped to 0-100 intgere values.",
            "data_type": np.float32,
            "min_value": 0,
            "max_value": 100,
            "fill_value": np.nan
        },
        "reliability": {
            "band_index": 2,
            "description": "Distance between the couple of years for which the change "
                           "has been calculated",
            "data_type": np.float32,
            "min_value": 0,
            "max_value": 5,
            "fill_value": np.nan
        },
        "pcc_priority_changes": {
            "band_index": 3,
            "description": "From Post Classification Comparison of HRLC30 maps, "
                           "the class cover transition of the change in its degree of "
                           "priority. 0=no change, 1=low priority change, "
                           "2=high priority change.",
            "data_type": np.float32,
            "min_value": 0,
            "max_value": 2,
        }
    },
    "MAP-CL01": {
        "CL01": {
            "band_index": 0,
            "description": "First class as returned by the classification algorithm "
                           "according to posterior probability ranking.",
            "data_type": np.byte,
            "min_value": 0,
            "max_value": 150,
        }
    },
    "UNCERT-CL02": {
        "CL02": {
            "band_index": 0,
            "description": "Second class as returned by the classification algorithm "
                           "according to posterior probability ranking.",
            "data_type": np.byte,
            "min_value": 0,
            "max_value": 150,
        }
    },
    "UNCERT-PS01": {
        "PS01": {
            "band_index": 0,
            "description": "Posterior probability associated to the first ranked "
                           "class. Original probability values in the range [0,1] "
                           "are mapped to 0-100 integer values.",
            "data_type": np.byte,
            "min_value": 0,
            "max_value": 100,
        }
    },
    "UNCERT-PS02": {
        "PS02": {
            "band_index": 0,
            "description": "Posterior probability associated to the second ranked "
                           "class. Original probability values in the range [0,1] "
                           "are mapped to 0-100 integer values.",
            "data_type": np.byte,
            "min_value": 0,
            "max_value": 100,
        }
    },
    "UNCERT-IQIX": {
        "IQIX": {
            "band_index": 0,
            "description": "Optical imagery input quality evaluated before "
                           "classification by considering the number of valid images "
                           "at pixel level used for classification. Ranges from 0–low "
                           "quality to 3–high quality",
            "data_type": np.byte,
            "min_value": 0,
            "max_value": 3,
        }
    },
    "CL": {
        "CL": {}
    },
    "JD": {
        "JD": {}
    },
    "LC": {
        "LC": {}
    }
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
