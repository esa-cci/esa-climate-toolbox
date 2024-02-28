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

from xcube.util.assertions import assert_given


def ect_init():
    # Plugin initializer.
    import yaml
    import os
    from xcube.core.store import DataStoreConfig
    from xcube.core.store import get_data_store_params_schema

    from esa_climate_toolbox.conf import get_data_stores_path
    from esa_climate_toolbox.conf.defaults import STORES_CONF_FILE
    from esa_climate_toolbox.core.common import default_user_agent
    from esa_climate_toolbox.core.ds import ECT_DATA_STORE_POOL

    dir_path = os.path.dirname(os.path.abspath(__file__))
    default_stores_file = os.path.join(dir_path, 'data/stores.yml')

    configured_store_configs = {}
    if os.path.exists(STORES_CONF_FILE):
        with open(STORES_CONF_FILE, 'r') as fp:
            configured_store_configs = yaml.safe_load(fp)
    with open(default_stores_file, 'r') as fp:
        default_store_configs = yaml.safe_load(fp)
    store_configs = configured_store_configs | default_store_configs

    for store_name, store_config in store_configs.items():
        store_id = store_config.get('store_id')
        assert_given(store_id, name='store_id', exception_type=RuntimeError)

        if store_id == 'file' \
                and 'store_params' in store_config \
                and store_config.get('store_params', {}).get('root') is None:
            root = os.environ.get('ECT_LOCAL_DATA_STORE_PATH',
                                  os.path.join(get_data_stores_path(),
                                               store_name))
            # Note: even if the root directory doesn't exist yet,
            # the xcube "file" data store will create it for us.
            store_config['store_params']['root'] = root

        store_params_schema = get_data_store_params_schema(store_id)
        if 'user_agent' in store_params_schema.properties:
            if 'store_params' not in store_config:
                store_config['store_params'] = {}
            store_config['store_params']['user_agent'] = default_user_agent()

        store_config = DataStoreConfig.from_dict(store_config)
        ECT_DATA_STORE_POOL.add_store_config(store_name, store_config)
