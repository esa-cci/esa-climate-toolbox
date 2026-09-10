import numpy.testing as nt
from unittest import TestCase

from datetime import datetime
import numpy as np
import xarray as xr

from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.ops import agb_change_op


class BiomassTestCase(TestCase):
    def setUp(self):
        agb_data = np.arange(5000, 5025)
        agb_data = np.concat([agb_data, np.flip(agb_data) + 5, agb_data])
        agb_data[3] = agb_data[28] = 0
        agb_data = agb_data.reshape(3, 5, 5)
        self.biomass_ds = xr.Dataset(
            data_vars= {
                'agb': (('time', 'lat', 'lon'), agb_data),
                'agb_sd': (('time', 'lat', 'lon'), np.full((3, 5, 5), 8))
            },
            coords={
                'time':  [datetime(year, 1, 1) for year in range(2013, 2018, 2)],
                'lat': [r / 10 for r in range(75, 70, -1)],
                'lon': [r / 10 for r in range(-775, -770, 1)]
            },
        )

    def test_biomass_change_op_wrong_reference(self):
        with self.assertRaises(ValidationError) as error:
            agb_change_op(
                self.biomass_ds,
                reference=2005,
                difference=2015,
            )
        self.assertIn("Reference time not in dataset", str(error.exception))


    def test_biomass_change_op_wrong_difference(self):
        with self.assertRaises(ValidationError) as error:
            agb_change_op(
                self.biomass_ds,
                reference=2015,
                difference= 2025,
            )
        self.assertIn("Difference time not in dataset", str(error.exception))

    def test_biomass_change_op_missing_agb_name(self):
        with self.assertRaises(ValidationError) as error:
            agb_change_op(
                self.biomass_ds,
                reference=2013,
                difference= 2015,
                agb_name="wrong"
            )
        self.assertIn("Variable 'wrong' not in dataset", str(error.exception))

    def test_biomass_change_op_missing_agb_sd_name(self):
        with self.assertRaises(ValidationError) as error:
            agb_change_op(
                self.biomass_ds,
                reference=2013,
                difference= 2015,
                agb_sd_name="wrong"
            )
        self.assertIn("Variable 'wrong' not in dataset", str(error.exception))

    def test_biomass_change_op(self):
        agb_ds = agb_change_op(
            self.biomass_ds,
            reference=2013,
            difference= 2015
        )
        self.assertIsNotNone(agb_ds)
        self.assertSetEqual({"diff", "diff_sd", "diff_qf"}, set(agb_ds.data_vars))
        self.assertEqual("AGB Difference", agb_ds["diff"].attrs.get("long_name"))
        self.assertEqual("Standard Deviation", agb_ds["diff_sd"].attrs.get("long_name"))
        self.assertEqual("Quality Flag", agb_ds["diff_qf"].attrs.get("long_name"))
        self.assertListEqual(
            [0, 1, 2, 3, 4, 5],
            agb_ds["diff_qf"].attrs.get("flag_values")
        )
        self.assertListEqual(
            ["AGB=0 t/ha in both maps", "significant loss", "potential loss",
            "non significant change", "potential gain", "significant gain"],
            agb_ds["diff_qf"].attrs.get("flag_meanings")
        )
        nt.assert_array_equal(
            np.array([
                [29, 27, 25, 0, 21],
                [19, 17, 15, 13, 11],
                [9, 7, 5, 3, 1],
                [-1, -3, -5, -7, -9],
                [-11, -13, -15, -17, -19]]
            ),
            agb_ds["diff"].values
        )
        nt.assert_array_equal(
            np.array([
                [11, 11, 11, 11, 11],
                [11, 11, 11, 11, 11],
                [11, 11, 11, 11, 11],
                [11, 11, 11, 11, 11],
                [11, 11, 11, 11, 11]]
            ),
            agb_ds["diff_sd"].values
        )
        nt.assert_array_equal(
            np.array([
                [3, 3, 3, 0, 3],
                [5, 3, 3, 3, 3],
                [4, 3, 3, 3, 3],
                [3, 3, 3, 3, 2],
                [2, 2, 2, 1, 1]]
            ),
            agb_ds["diff_qf"].values
        )
