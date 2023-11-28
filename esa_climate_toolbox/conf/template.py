################################################################################
# This is an ESA Climate Toolbox configuration file.                           #
#                                                                              #
# If this file is "~/.ect/conf.py", it is the active configuration.            #
#                                                                              #
# If this file is named "conf.py.template" you can rename it and move          #
# it to "~/.ect/conf.py" to make it the active configuration file.             #
#                                                                              #
# As this is a regular Python script, you may use any Python code to compute   #
# the settings provided here.                                                  #
#                                                                              #
################################################################################

# 'data_stores_path' is denotes a directory where the ESA Climate Toolbox
# stores information about data stores and also saves local data files
# synchronized with their remote versions.
# Use the tilde '~' (also on Windows) within the path to point to your home
# directory.
#
# data_stores_path = '~/.ect/data_stores'

# Default color map to be used for any variable not configured in
# 'variable_display_settings'
# 'default_color_map' must be the name of a color map taken from from
# https://matplotlib.org/examples/color/colormaps_reference.html
# default_color_map = 'jet'
default_color_map = 'inferno'

# Data Store Configurations
# Load from here the configurations of the data stores that will eventually be
# loaded into the ESA Climate Toolbox
store_configs = {
    "local": {
        "store_id": "file",
        "store_params": {
            "root": "",
        }
     },
     "cci-store": {
        "store_id": "esa-cdc"
     },
     "cci-zarr-store": {
        "store_id": "esa-cdc-zarr"
    }
}
