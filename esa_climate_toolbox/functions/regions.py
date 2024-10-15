import importlib.resources
from typing import Union, List

import numpy as np
import pandas as pd
from shapely.ops import unary_union
import xarray as xr

from xcube.core.geom import mask_dataset_by_geometry
from xcube.core.geom import rasterize_features
from xcube.core.store import new_data_store
from xcube.util.assertions import assert_instance


def _get_countries_df():
    with importlib.resources.path(
            "esa_climate_toolbox.functions.country_data", "README.md"
    ) as p:
        countries_path = str(p.parent)
    countries_store = new_data_store("file", root=countries_path)
    return countries_store.open_data(f"countries.geojson")


def make_regions_dataset(
        template: xr.Dataset, regions: Union[str, List[str]] = None
) -> xr.Dataset:
    """Rasterize country and continent polygons into a grid provided
    by a *template* dataset.

    The returned dataset has the same gridmapping and same
    spatial chunking as *template* and contains two variables:

    * "country_code" provides an integer country code.
      The names of countries are provided by its attribute "country_names".
    * "continent_code" provides an integer continent code.
      The names of continent are provided by its attribute "continent_names".

    :param template: The dataset to which the regions dataset shall refer
    :param regions: A single name of a country or continent or a list of names.
        If given, only these regions will be considered. If omitted, all countries
        will be included. Default is None.
    :return: A dataset with the same grid mapping as *template*
    """
    assert_instance(template, xr.Dataset, "template")

    if regions is not None and not isinstance(regions, List):
        regions = list(regions)

    countries = _get_countries_df()
    if regions:
        countries_subset = countries[countries['name'].isin(regions)]
        continents_subset = countries[countries['continent'].isin(regions)]
        countries = pd.concat([countries_subset, continents_subset])
        countries = countries.reset_index(drop=True)
    country_names = {code + 1: name for code, name in enumerate(countries['name'])}
    continent_names = {code + 1: name for code, name in
                       enumerate(set(countries['continent']))}

    countries["country_code"] = [code for code in country_names.keys()]
    continent_codes = []
    for continent_name in countries["continent"]:
        for c_code, c_name in continent_names.items():
            if continent_name == c_name:
                continent_codes.append(c_code)
                break
    countries["continent_code"] = continent_codes

    countries_and_continents_dataset = rasterize_features(
        template,
        countries,
        ["country_code", "continent_code"],
        var_props={
            "country_code": {
                "name": "country_code",
                "dtype": np.uint8,
                "fill_value": 255
            },
            "continent_code": {
                "name": "continent_code",
                "dtype": np.uint8,
                "fill_value": 255
            }
        }
    )
    countries_and_continents_dataset = (
        countries_and_continents_dataset.drop_vars("time"))
    country_code = countries_and_continents_dataset.country_code
    country_code.attrs["country_names"] = country_names
    continent_code = countries_and_continents_dataset.continent_code
    continent_code.attrs["continent_names"] = continent_names

    return countries_and_continents_dataset


def get_land_mask(dataset: xr.Dataset):
    """Gets a land mask for a dataset.

    The returned dataset has the same grid mapping and same
    spatial chunking as *template* and contains one variable:

    * "land" provides a mask that is True if the pixel is on land, otherwise False.

    :param dataset: The dataset for which the mask shall be created
    :return: A dataset with the same grid mapping as *dataset*
    """
    countries_dataset = make_regions_dataset(dataset)
    countries_dataset['land'] = xr.where(
        countries_dataset.country_code >= 0, True, False
    )
    countries_dataset = countries_dataset.drop_vars("country_code")
    countries_dataset = countries_dataset.drop_vars("continent_code")
    return countries_dataset


def get_regions_mask(dataset: xr.Dataset, regions: Union[str, List[str]]):
    """Gets a regions mask for a dataset.

    The returned dataset has the same gridmapping and same
    spatial chunking as *template* and contains one variable:

    * "regions" provides a mask that is True if the pixel is in one of the specified
     regions, otherwise False.

    :param dataset: The dataset for which the mask shall be created
    :param regions: A single name of a country or continent or a list of names.
    :return: A dataset with the same grid mapping as *dataset*
    """
    countries_dataset = make_regions_dataset(dataset, regions)

    countries_dataset['regions'] = xr.where(
        countries_dataset.country_code + countries_dataset.continent_code >= 0,
        True, False
    )
    countries_dataset = countries_dataset.drop_vars("country_code")
    countries_dataset = countries_dataset.drop_vars("continent_code")
    return countries_dataset


def mask_dataset_by_land(dataset: xr.Dataset):
    """Masks out non-land-pixels of a dataset.

    :param dataset: The dataset for which the mask shall be created
    :return: A dataset which only has values for pixels on land
    """
    countries = _get_countries_df()
    land_geometry = unary_union(countries.geometry)
    return mask_dataset_by_geometry(dataset, land_geometry)


def mask_dataset_by_regions(dataset: xr.Dataset, regions: Union[str, List[str]]):
    """Masks out pixels of a dataset which are not in one of the specified regions.

    :param dataset: The dataset for which the mask shall be created
    :param regions: A single name of a country or continent or a list of names.
    :return: A dataset which only has values for pixels in one of the specified regions.
    """
    countries = _get_countries_df()

    countries_subset = countries[countries['name'].isin(regions)]
    continents_subset = countries[countries['continent'].isin(regions)]
    sub_df = pd.concat([countries_subset, continents_subset])

    sub_geometry = unary_union(sub_df.geometry)
    return mask_dataset_by_geometry(dataset, sub_geometry)
