import numpy as np
import numpy.testing as nt
import unittest

from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.util.im.cschemes import CategoricalColorScheme
from esa_climate_toolbox.util.im.cschemes import CategoricalContinuousColorScheme
from esa_climate_toolbox.util.im.cschemes import COLOR_SCHEME_REGISTRY

class CategoricalColorSchemeTest(unittest.TestCase):

    def test_empty_scheme(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorScheme("faulty_ccs", "faulty_ccm", [], [], [])
        self.assertEqual(str(ve.exception), "Categorical Color Scheme must contain at least one value.")

    def test_scheme_with_unequal_values(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorScheme("faulty_ccs", "faulty_ccm", [0, 10], ["red", "green", "blue"])
        self.assertEqual(
            str(ve.exception),
            "Number of color scheme values (2) and number of colors (3) must be equal."
        )

    def test_scheme_with_unequal_values_and_labels(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalColorScheme("faulty_ccs", "faulty_ccm", [0, 10], ["red", "green", "blue"], ["First", "Second"])
        self.assertEqual(
            str(ve.exception),
            "Number of color scheme values (2), number of labels (2) and number of colors (3) must be equal."
        )

    def test_simple_scheme(self):
        ccs = CategoricalColorScheme(
            "test_ccs", "test_ccm", [0, 10, 15], ["red", "green", "blue"], ["First", "Second", "Third"]
        )
        self.assertEqual("test_ccs", ccs.color_scheme_name)
        self.assertEqual("test_ccm", ccs.color_map_name)
        nt.assert_array_equal(np.array([0, 10, 15]), ccs.values)
        self.assertEqual(["red", "green", "blue"], ccs.colors)
        self.assertEqual(["First", "Second", "Third"], ccs.labels)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), ccs.tick_values)


class CategoricalContinuousColorSchemeTest(unittest.TestCase):

    def test_empty_scheme(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalContinuousColorScheme("faulty_ccs", "faulty_ccm", [], [], [])
        self.assertEqual(str(ve.exception), "Categorical Color Scheme must contain at least one value.")

    def test_scheme_with_unequal_values(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalContinuousColorScheme("faulty_ccs", "faulty_ccm", [0, 10], ["red", "green", "blue"])
        self.assertEqual(
            str(ve.exception),
            "Number of color scheme values (2) and number of colors (3) must be equal."
        )

    def test_scheme_with_unequal_values_and_labels(self):
        with self.assertRaises(ValidationError) as ve:
            CategoricalContinuousColorScheme(
                "faulty_ccs", "faulty_ccm", [0, 10], ["red", "green", "blue"], ["First", "Second"]
            )
        self.assertEqual(
            str(ve.exception),
            "Number of color scheme values (2), number of labels (2) and number of colors (3) must be equal."
        )

    def test_simple_scheme(self):
        cccs = CategoricalContinuousColorScheme(
            "test_ccs", "test_ccm", [0, 10, 15], ["red", "green", "blue"], ["First", "Second", "Third"]
        )
        self.assertEqual("test_ccs", cccs.color_scheme_name)
        self.assertEqual("test_ccm_base", cccs.color_map_name)
        nt.assert_array_equal(np.array([0, 10, 15]), cccs.values)
        self.assertEqual(["red", "green", "blue"], cccs.colors)
        self.assertEqual(["First", "Second", "Third"], cccs.labels)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), cccs.tick_values)

    def test_set_continuous_values(self):
        cccs = CategoricalContinuousColorScheme(
            "test_ccs", "test_ccm", [0, 10, 15], ["red", "green", "blue"], ["First", "Second", "Third"], "Reds"
        )

        cont_values = [20, 22, 23, 26]
        cccs.set_continuous_values(cont_values, range_extent=5, label_shift=15, labels_space=2)

        self.assertEqual("test_ccs", cccs.color_scheme_name)
        self.assertNotEqual("test_ccm_base", cccs.color_map_name)
        self.assertTrue(cccs.color_map_name.startswith("test_ccm_"))
        nt.assert_array_equal(np.array([0, 10, 15, 20, 22, 23, 26]), cccs.values)
        self.assertEqual(["First", "Second", "Third", "", "7", "", "11"], cccs.labels)
        self.assertEqual(["red", "green", "blue"], cccs.colors[:3])
        self.assertEqual(7, len(cccs.colors))
        nt.assert_array_equal(np.array([2.25, 8.75, 15.0, 19.25, 21.75, 23.5, 25.5]), cccs.tick_values)


class ColorSchemeRegistryTest(unittest.TestCase):

    def test_get_categorical_scheme(self):
        lc_map = COLOR_SCHEME_REGISTRY.get_color_scheme("land_cover_fire_cci")
        self.assertEqual("land_cover_fire_cci", lc_map.color_scheme_name)
        self.assertEqual("land_cover_fire_cci", lc_map.color_map_name)
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

    def test_get_categorical_continuous_scheme(self):
        jd_scheme = COLOR_SCHEME_REGISTRY.get_color_scheme("fire_jd_cci")
        self.assertEqual("fire_jd_cci", jd_scheme.color_scheme_name)
        self.assertEqual("fire_jd_cci_base", jd_scheme.color_map_name)
        self.assertEqual(["Non-burnable", "Not observed", "Unburnt"], jd_scheme.labels)
        self.assertEqual(['#1e90ff', '#91a3b0', '#000000'], jd_scheme.colors)
        nt.assert_array_equal(np.array([-2, -1, 0]), jd_scheme.values)
        nt.assert_array_equal(np.array([-2, -1, 0]), jd_scheme.tick_values)

    def test_register_and_get_categorical_scheme(self):
        with self.assertRaises(KeyError):
            COLOR_SCHEME_REGISTRY.get_color_scheme("test_ccs_2")

        COLOR_SCHEME_REGISTRY.register_categorical_color_scheme(
            [0, 10, 15], "test_ccs_2", "test_ccm_2", ["red", "green", "blue"], ["First", "Second", "Third"]
        )

        cc_scheme = COLOR_SCHEME_REGISTRY.get_color_scheme("test_ccs_2")

        self.assertEqual("test_ccs_2", cc_scheme.color_scheme_name)
        self.assertEqual("test_ccm_2", cc_scheme.color_map_name)
        self.assertEqual(["First", "Second", "Third"], cc_scheme.labels)
        self.assertEqual(["red", "green", "blue"], cc_scheme.colors)
        nt.assert_array_equal(np.array([0, 10, 15]), cc_scheme.values)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), cc_scheme.tick_values)

        COLOR_SCHEME_REGISTRY.deregister_color_scheme("test_ccs_2")

        with self.assertRaises(KeyError):
            COLOR_SCHEME_REGISTRY.get_color_scheme("test_ccs_2")

    def test_register_and_get_categorical_continuous_scheme(self):
        with self.assertRaises(KeyError):
            COLOR_SCHEME_REGISTRY.get_color_scheme("test_cccs_2")

        COLOR_SCHEME_REGISTRY.register_categorical_continuous_color_scheme(
            [0, 10, 15], "test_cccs_2", "test_ccm_2", "Reds", ["red", "green", "blue"], ["First", "Second", "Third"]
        )

        cc_scheme = COLOR_SCHEME_REGISTRY.get_color_scheme("test_cccs_2")

        self.assertEqual("test_cccs_2", cc_scheme.color_scheme_name)
        self.assertEqual("test_ccm_2_base", cc_scheme.color_map_name)
        self.assertEqual(["First", "Second", "Third"], cc_scheme.labels)
        self.assertEqual(["red", "green", "blue"], cc_scheme.colors)
        nt.assert_array_equal(np.array([0, 10, 15]), cc_scheme.values)
        nt.assert_array_equal(np.array([2.25, 8.75, 14.0]), cc_scheme.tick_values)

        cc_scheme.set_continuous_values([20, 30, 35, 40], range_extent=6, label_shift=-5, labels_space=1)

        self.assertEqual("test_cccs_2", cc_scheme.color_scheme_name)
        self.assertNotEqual("test_ccm_2_base", cc_scheme.color_map_name)
        self.assertTrue(cc_scheme.color_map_name.startswith("test_ccm_2"))
        self.assertEqual(["First", "Second", "Third", "25", "35", "40", "45"], cc_scheme.labels)
        self.assertEqual(7, len(cc_scheme.colors))
        self.assertEqual(["red", "green", "blue"], cc_scheme.colors[:3])
        nt.assert_array_equal(np.array([0, 10, 15, 20, 30, 35, 40]), cc_scheme.values)
        nt.assert_array_equal(np.array([2.25, 8.75, 15., 21.25, 28.75, 35., 39.]), cc_scheme.tick_values)

        COLOR_SCHEME_REGISTRY.deregister_color_scheme("test_cccs_2")

        with self.assertRaises(KeyError):
            COLOR_SCHEME_REGISTRY.get_color_scheme("test_cccs_2")
