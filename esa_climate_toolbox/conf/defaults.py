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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import os.path

from ..version import __version__

DEFAULT_DATA_DIR_NAME = '.ect'
DEFAULT_DATA_PATH = os.path.join(os.path.expanduser('~'), DEFAULT_DATA_DIR_NAME)
DEFAULT_VERSION_DATA_PATH = os.path.join(DEFAULT_DATA_PATH, __version__)

GLOBAL_CONF_FILE = os.path.join(DEFAULT_DATA_PATH, 'conf.py')
STORES_CONF_FILE = os.environ.get(
    'ECT_STORES_CONFIG_PATH', os.path.join(DEFAULT_DATA_PATH, 'stores.yml')
)
VERSION_CONF_FILE = os.path.join(DEFAULT_VERSION_DATA_PATH, 'conf.py')
LOCAL_CONF_FILE = 'ect-conf.py'
LOCATION_FILE = 'ect.location'
USER_PREFERENCES_FILE = 'user-preferences.json'

DEFAULT_RES_PATTERN = 'res_{index}'

NETCDF_COMPRESSION_LEVEL = 9

_ONE_MIB = 1024 * 1024
_ONE_GIB = 1024 * _ONE_MIB

DEFAULT_COLOR_MAP = 'inferno'
