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

"""
Description
===========

The ``esa_climate_toolbox.core.plugin`` module exposes the Toolbox plugin
``REGISTRY`` which is mapping from the toolbox entry point names to plugin meta
information. A Toolbox plugin is any callable in an internal/extension module
registered with ``esa_climate_toolbox_plugins`` entry point.

Clients register a Toolbox plugin in the ``setup()`` call of their ``setup.py``
script. The following plugin example comprises a main module
``wavelet_gapfill`` which provides the entry point function ``ect_init``:::

    setup(
        name="gapfill-wavelet",
        version="0.5",
        description='A wavelet-based gap-filling algorithm',
        license='GPL 3',
        author='John Doe',
        packages=['wavelet_gapfill'],
        entry_points={
            'esa_climate_toolbox_plugins': [
                'wavelet_gapfill = wavelet_gapfill:ect_init',
            ],
        },
        install_requires=['pywavelets >= 2.1'],
    )

The entry point callable should have the following signature::

    def ect_init(*args, **kwargs):
        pass

or::

    class EctInit:
        def __init__(*args, **kwargs)__:
            pass

The return values are ignored.

Verification
============

The module's unit-tests are located in
`test/cire/test_plugin.py <https://github.com/esa-cci/esa-climate-toolbox/tree/main/test/core/test_plugin.py>`_
and may be executed using
``$ py.test test/core/test_plugin.py --cov=esa_climate_toolbox/core/plugin.py``
for extra code coverage information.

Components
==========
"""

import logging
from collections import OrderedDict

from importlib.metadata import entry_points

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = logging.getLogger('ect')


def _load_plugins():
    plugins = OrderedDict()
    for entry_point in entry_points().select(
            group='esa_climate_toolbox_plugins'
    ):

        # noinspection PyBroadException
        try:
            plugin = entry_point.load()
        except Exception:
            _LOG.exception(
                "unexpected exception while loading ESA Climate Toolbox plugin "
                "with entry point '%s'" % entry_point.name)
            continue

        if callable(plugin):
            # noinspection PyBroadException
            try:
                plugin()
            except Exception:
                _LOG.exception(
                    "unexpected exception while executing ESA Climate Toolbox "
                    "plugin with entry point '%s'" % entry_point.name)
                continue
        else:
            _LOG.error("ESA Climate Toolbox plugin with entry point '%s' "
                       "must be a callable but got a '%s'" %
                       (entry_point.name, type(plugin)))
            continue

        # Here: use pkg_resources and introspection to generate a
        # JSON-serializable dictionary of plugin meta-information
        plugins[entry_point.name] = {'entry_point': entry_point.name}

    return plugins


def ect_init(*arg, **kwargs):
    """
    No actual use, just demonstrates the signature of an entry point callable.

    :param arg: any arguments (not used)
    :param kwargs: any keyword arguments (not used)
    :return: any or void (not used)
    """
    return arg, kwargs


#: Mapping of ESA Climate Toolbox entry point names to JSON-serializable plugin
# meta-information.
PLUGIN_REGISTRY = _load_plugins()
