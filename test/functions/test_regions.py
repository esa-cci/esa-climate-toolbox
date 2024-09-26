# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
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

import unittest

from xcube.core.new import new_cube

from esa_climate_toolbox.functions.regions import get_land_mask
from esa_climate_toolbox.functions.regions import get_regions_mask
from esa_climate_toolbox.functions.regions import make_regions_dataset
from esa_climate_toolbox.functions.regions import mask_dataset_by_land
from esa_climate_toolbox.functions.regions import mask_dataset_by_regions


class CountriesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cube = new_cube(
            width=36, height=18, x_res=10.0, y_res=10.0, variables={'s': 2.0}
        )

    def test_make_regions_dataset(self):
        regions_dataset = make_regions_dataset(self.cube)

        self.assertIsNotNone(regions_dataset)
        self.assertEqual(2, len(regions_dataset.data_vars))
        self.assertIn("country_code", regions_dataset.data_vars)
        self.assertIn("continent_code", regions_dataset.data_vars)

    def test_get_land_mask(self):
        land_mask = get_land_mask(self.cube)

        self.assertIsNotNone(land_mask)
        self.assertEqual(1, len(land_mask.data_vars))
        self.assertIn("land", land_mask.data_vars)

    def test_get_regions_mask(self):
        regions_mask = get_regions_mask(self.cube, regions=["Oceania", "South Africa"])

        self.assertIsNotNone(regions_mask)
        self.assertEqual(1, len(regions_mask.data_vars))
        self.assertIn("regions", regions_mask.data_vars)

    def test_mask_dataset_by_land(self):
        masked_ds = mask_dataset_by_land(self.cube)

        self.assertIsNotNone(masked_ds)
        self.assertEqual(1, len(masked_ds.data_vars))
        self.assertIn("s", masked_ds.data_vars)

    def test_mask_dataset_by_regions(self):
        masked_ds = mask_dataset_by_regions(
            self.cube, regions=["Oceania", "South Africa"]
        )

        self.assertIsNotNone(masked_ds)
        self.assertEqual(1, len(masked_ds.data_vars))
        self.assertIn("s", masked_ds.data_vars)
