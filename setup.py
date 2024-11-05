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

from setuptools import setup, find_packages

requirements = [
    # Sync with ./environment.yml.
]

packages = find_packages(exclude=["test", "test.*"])

# Same effect as "from esa_climate_toolbox import version",
# but avoids importing esa_climate_toolbox:
__version__ = None
with open('esa_climate_toolbox/version.py') as f:
    exec(f.read())

setup(
    name="esa_climate_toolbox",
    version=__version__,
    description='A toolbox that provides access to data from the ESA Open '
                'Data Portal as well as functions to work with this data.',
    license='MIT',
    author='CCI Toolbox Development Team',
    packages=packages,
    include_package_data=True,
    data_files=[(
        'esa_climate_toolbox', [
            'esa_climate_toolbox/ds/data/stores.yml'
            ]
    )],
    install_requires=requirements,
    entry_points={
        'esa_climate_toolbox_plugins': [
            'esa_climate_toolbox_ops = esa_climate_toolbox.ops:ect_init',
            'esa_climate_toolbox_ds = esa_climate_toolbox.ds:ect_init',
        ],
    }
)
