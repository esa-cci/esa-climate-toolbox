# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
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
import os.path
import shutil
import unittest

import pandas as pd

from xcube.core.new import new_cube

from esa_climate_toolbox.functions.outputarray import output_array_as_image
from esa_climate_toolbox.functions.outputarray import output_animation
from esa_climate_toolbox.functions.outputarray import output_dataset_as_geotiff

class OutputArrayTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.makedirs("test_output_array")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("test_output_array")

    def setUp(self):
        self._cube = new_cube(variables=dict(a=1.5, b=0.1))

    def test_output_wrong_array(self):
        with self.assertRaises(ValueError) as ve:
            output_array_as_image(self._cube.a)
        self.assertEqual("Function is only available for 2-dimensional data arrays,"
                         f"array has dimensions 'time, lat, lon'", f"{ve.exception}")

    def test_output_wrong_format(self):
        with self.assertRaises(ValueError) as ve:
            output_array_as_image(self._cube.a.isel(time=0), out_format="jpeg")
        self.assertEqual("Unsupported output format. Only support 'png' and 'gtiff', "
                         f"found 'jpeg'.", f"{ve.exception}")

    def test_output_png(self):
        out_path = "test_output_array/a.png"
        self.assertFalse(os.path.exists(out_path))
        output_array_as_image(self._cube.a.isel(time=0), filepath=out_path, out_format="png")
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)

    def test_output_geotiff(self):
        out_path = "test_output_array/a.gtiff"
        self.assertFalse(os.path.exists(out_path))
        output_array_as_image(self._cube.a.isel(time=0), filepath=out_path, out_format="gtiff")
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)


class OutputDataArraysAsGeotiffTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.makedirs("test_output_daag", exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("test_output_daag")

    def setUp(self):
        self._cube = new_cube(variables=dict(a=1.5, b=0.1))

    def test_output_wrong_dataset(self):
        with self.assertRaises(ValueError) as ve:
            output_dataset_as_geotiff(self._cube)
        self.assertEqual("No appropriate variables found in dataset.", f"{ve.exception}")

    def test_output_dataarrays_as_geotif(self):
        out_path = "test_output_daag/ab.gtiff"
        self.assertFalse(os.path.exists(out_path))
        actual_out_path = output_dataset_as_geotiff(
            self._cube.isel(time=0), filepath=out_path
        )
        self.assertEqual(out_path, actual_out_path)
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)

    def test_output_dataarrays_as_geotif_var_names_restricted(self):
        out_path = "test_output_daag/a.gtiff"
        self.assertFalse(os.path.exists(out_path))
        actual_out_path = output_dataset_as_geotiff(
            self._cube.isel(time=0), var_names=["a"], filepath=out_path
        )
        self.assertEqual(out_path, actual_out_path)
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)

    def test_output_dataarrays_as_geotif_var_names_timestamp(self):
        out_path = "test_output_daag/ab.gtiff"
        self.assertFalse(os.path.exists(out_path))
        actual_out_path = output_dataset_as_geotiff(
            self._cube, timestamp=pd.Timestamp("2010-01-02"), filepath=out_path
        )
        self.assertEqual(out_path, actual_out_path)
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)

    def test_output_dataarrays_as_geotif_var_names_timeindex(self):
        out_path = "test_output_daag/ab.gtiff"
        self.assertFalse(os.path.exists(out_path))
        actual_out_path = output_dataset_as_geotiff(
            self._cube, timeindex=2, filepath=out_path
        )
        self.assertEqual(out_path, actual_out_path)
        self.assertTrue(os.path.exists(out_path))
        os.remove(out_path)


class OutputAnimationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.makedirs("test_output_animation")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("test_output_animation")

    def setUp(self):
        self._cube = new_cube(variables=dict(a=1.5, b=0.1))

    def test_output_wrong_format(self):
        with self.assertRaises(ValueError) as ve:
            output_animation(self._cube.a, img_format="jpeg")
        self.assertEqual("Unsupported output format. Only support 'png' and 'gtiff', "
                         f"found 'jpeg'.", f"{ve.exception}")

    def test_output_png(self):
        out_path = "test_output_animation/a/"
        # self.assertFalse(os.path.exists(out_path))
        output_animation(self._cube.a, output_folder=out_path)
        self.assertTrue(os.path.exists(out_path))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-01T12:00:00.png"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-02T12:00:00.png"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-03T12:00:00.png"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-04T12:00:00.png"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-05T12:00:00.png"))

    def test_output_gtiff(self):
        out_path = "test_output_animation/a/"
        # self.assertFalse(os.path.exists(out_path))
        output_animation(self._cube.a, output_folder=out_path, img_format="gtiff")
        self.assertTrue(os.path.exists(out_path))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-01T12:00:00.gtiff"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-02T12:00:00.gtiff"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-03T12:00:00.gtiff"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-04T12:00:00.gtiff"))
        self.assertTrue(os.path.exists(f"{out_path}2010-01-05T12:00:00.gtiff"))
