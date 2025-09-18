# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
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

Provides various resampling methods including up- and downsampling for temporal resampling.

Components
==========
"""

import xarray as xr

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.util.monitor import Monitor
from esa_climate_toolbox.core.types import DatasetLike, ValidationError, VarNamesLike


@op(tags=['correlation', "multivariate"], version='1.0')
@op_input('ds_1', data_type=DatasetLike)
@op_input('ds_1_var_names', data_type=VarNamesLike)
@op_input('ds_2', data_type=DatasetLike)
@op_input('ds_2_var_names', data_type=VarNamesLike)
@op_return(add_history=True)
def pairwise_var_correlation(
        ds_1: DatasetLike.TYPE,
        ds_2: DatasetLike.TYPE,
        ds_1_var_names: VarNamesLike.TYPE=None,
        ds_2_var_names: VarNamesLike.TYPE=None,
) -> xr.Dataset:
    """
    Computes correlations between each pair of variables of two datasets.
    Note that the datasets are expected to have the same spatial and temporal
    grids. consider applying 'temporal_alignment' and 'coregistration' before
    using this operation.

    :param ds_1: The first dataset.
    :param ds_2: The second dataset.
    :param ds_1_var_names: The names of the variables in the first dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param ds_2_var_names: The names of the variables in the second dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.

    :return: A new dataset with n*m data arrays, where n is the number of
        variables from the first dataset and m is the number of variables from
        the second dataset. Each data array's name will indicate the combination
        of variables.
    """
    # out = {}
    # for a in varsA:
    #     for b in varsB:
    #         name = f"{a}__{b}"
    #         out[name] = xr.corr(dsA[a], dsB[b], dim=dim)
    # return xr.Dataset(out)
    pass

@op(tags=['correlation', 'multivariate'], version='1.0')
@op_input('ds_1', data_type=DatasetLike)
@op_input('ds_1_var_names', data_type=VarNamesLike)
@op_input('ds_2', data_type=DatasetLike)
@op_input('ds_2_var_names', data_type=VarNamesLike)
@op_return(add_history=True)
def pixelwise_group_correlation(
        ds_1: DatasetLike.TYPE,
        ds_2: DatasetLike.TYPE,
        ds_1_var_names: VarNamesLike.TYPE=None,
        ds_2_var_names: VarNamesLike.TYPE=None,
) -> xr.Dataset:
    """
    Computes for each pixel the correlation between groups of variables
    of two datasets. This method uses internally a canonical correlation
    analysis (CCA). The datasets are expected to have the same spatial and
    temporal grids. Consider applying 'temporal_alignment' and 'coregistration'
    before using this operation.

    :param ds_1: The first dataset.
    :param ds_2: The second dataset.
    :param ds_1_var_names: The names of the variables in the first dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param ds_2_var_names: The names of the variables in the second dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.

    :return: A new dataset with a single data array that shows for each pixel
        the correlation between variables from the first dataset and variables
        from the second dataset.
    """
    #     lat = dsA[varsA[0]].lat
    #     lon = dsA[varsA[0]].lon
    #     out = np.full((len(lat), len(lon)), np.nan)
    #
    #     for i, la in enumerate(lat):
    #         for j, lo in enumerate(lon):
    #             # Zeitreihen pro Pixel für Gruppe A und B
    #             X = np.stack([dsA[v].sel(lat=la, lon=lo).values for v in varsA], axis=1)  # (time, n_varsA)
    #             Y = np.stack([dsB[v].sel(lat=la, lon=lo).values for v in varsB], axis=1)  # (time, n_varsB)
    #             # Check: keine NaNs
    #             if np.isnan(X).any() or np.isnan(Y).any():
    #                 continue
    #             # Fit CCA für diesen Pixel
    #             cca = CCA(n_components=n_components)
    #             cca.fit(X, Y)
    #             U, V = cca.transform(X, Y)
    #             # Kanonische Korrelation der ersten Komponente
    #             corr = np.corrcoef(U[:,0], V[:,0])[0,1]
    #             out[i,j] = corr
    #
    #     return xr.DataArray(out, coords={"lat": lat, "lon": lon}, dims=("lat","lon"))
    pass


