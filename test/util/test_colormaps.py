import numpy as np
import numpy.testing as nt
import unittest

from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.util.colormaps import CategoricalColorMap
from esa_climate_toolbox.util.colormaps import deregister_color_map
from esa_climate_toolbox.util.colormaps import get_color_map
from esa_climate_toolbox.util.colormaps import register_categorical_color_map

class CategoricalColorMapTest(unittest.TestCase):

    def test_empty_map(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorMap("faulty_ccm", [], [], [])
        self.assertEqual(str(ve.exception), "Categorical Color Map must contain at least one value.")

    def test_map_with_unequal_values(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorMap("faulty_ccm", [0, 10], ["red", "green", "blue"])
        self.assertEqual(
            str(ve.exception),
            "Number of color map values (2) and number of colors (3) must be equal."
        )

    def test_map_with_unequal_values_and_labels(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorMap("faulty_ccm", [0, 10], ["red", "green", "blue"], ["First", "Second"])
        self.assertEqual(
            str(ve.exception),
            "Number of color map values (2), number of labels (2) and number of colors (3) must be equal."
        )

    def test_simple_map(self):
        ccm = CategoricalColorMap(
            "test_ccm", [0, 10, 15], ["red", "green", "blue"], ["First", "Second", "Third"]
        )

        self.assertEqual("test_ccm", ccm.name)
        nt.assert_array_equal(np.array([0, 10, 15]), ccm.values)
        self.assertEqual(["red", "green", "blue"], ccm.colors)
        self.assertEqual(["First", "Second", "Third"], ccm.labels)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), ccm.tick_values)


class CategoricalColorMapRegistrationTest(unittest.TestCase):

    def test_get_categorical_map(self):
        lc_map = get_color_map("land_cover_fire_cci")
        self.assertEqual("land_cover_fire_cci", lc_map.name)
        self.assertEqual(
            ['Unburnt', 'Cropland, rainfed', 'Cropland, irrigated or post-flooding',
             'Mosaic cropland (>50%) / natural vegetation (tree, shrub, herbaceous cover)(<50%)',
             'Mosaic natural vegetation (tree, shrub, herbaceous cover) (>50%) / cropland(<50%)',
             'Tree cover, broadleaved, evergreen, closed to open (>15%)',
             'Tree cover, broadleaved, deciduous, closed to open (>15%)',
             'Tree cover, needleleaved, evergreen, closed to open (>15%)',
             'Tree cover, needleleaved, deciduous, closed to open (>15%)',
             'Tree cover, mixed leaf type (broadleaved and needleleaved)',
             'Mosaic tree and shrub (>50%) / herbaceous cover (<50%)',
             'Mosaic herbaceous cover (>50%) / tree and shrub (<50%)', 'Shrubland', 'Grassland', 'Lichens and mosses',
             'Sparse vegetation (tree, shrub, herbaceous cover) (<15%)', 'Tree cover, flooded, fresh or brackish water',
             'Tree cover, flooded, saline water', 'Shrub or herbaceous cover, flooded, fresh/saline/brackish water'],
            lc_map.labels
        )
        self.assertEqual(
            ['#000000', '#FFFF64', '#AAF0F0', '#DCF064', '#C8C864', '#006400', '#00A000', '#003C00', '#285000',
             '#788200', '#8CA000', '#BE9600', '#966400', '#FFB432', '#FFDCD2', '#FFEBaf', '#0078BE', '#009678',
             '#00DC82'],
            lc_map.colors
        )
        nt.assert_array_equal(
            np.array([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180]),
            lc_map.values
        )
        nt.assert_array_equal(np.array([
            2.25, 10., 20., 30., 40., 50., 60., 70., 80., 90., 100., 110., 120., 130., 140., 150., 160., 170., 177.75
        ]),
            lc_map.tick_values
        )

    def test_register_and_get_categorical_map(self):
        with self.assertRaises(KeyError):
            get_color_map("test_ccm_2")

        register_categorical_color_map("test_ccm_2", [0, 10, 15], ["red", "green", "blue"], ["First", "Second", "Third"])

        cc_map = get_color_map("test_ccm_2")

        self.assertEqual("test_ccm_2", cc_map.name)
        self.assertEqual(["First", "Second", "Third"], cc_map.labels)
        self.assertEqual(["red", "green", "blue"], cc_map.colors)
        nt.assert_array_equal(np.array([0, 10, 15]), cc_map.values)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), cc_map.tick_values)

        deregister_color_map("test_ccm_2")

        with self.assertRaises(KeyError):
            get_color_map("test_ccm_2")
