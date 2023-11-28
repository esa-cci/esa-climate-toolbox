from unittest import TestCase

from esa_climate_toolbox.ds.normalize import normalize_dims_description
from esa_climate_toolbox.ds.normalize import normalize_variable_dims_description


class TestNormalize(TestCase):

    def test_normalize_dims(self):
        dims_1 = dict(lat=1, lon=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_1))

        dims_2 = dict(lat=1, lon=2, time=4, fgzrh=5, dfsraxt=6)
        self.assertEqual(dims_2, normalize_dims_description(dims_2))

        dims_3 = dict(latitude=1, lon=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_3))

        dims_4 = dict(lat=1, longitude=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_4))

        dims_5 = dict(latitude_centers=1, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_5))

        dims_6 = dict(latitude=1, dhft=2, time=4)
        self.assertEqual(dict(lat=1, dhft=2, time=4),
                         normalize_dims_description(dims_6))

    def test_normalize_variable_dims_description(self):
        dims_1 = ['time', 'lat', 'lon']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_1))

        dims_2 = ['lat', 'lon']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_2))

        dims_3 = ['latitude', 'longitude']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_3))

        dims_4 = ['latitude_centers']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_4))

        dims_5 = ['lat', 'lon', 'draeftgyhesj']
        self.assertEqual(['time', 'draeftgyhesj', 'lat', 'lon'],
                         normalize_variable_dims_description(dims_5))

        dims_6 = ['latitude_centers', 'draeftgyhesj']
        self.assertEqual(['time', 'draeftgyhesj', 'lat', 'lon'],
                         normalize_variable_dims_description(dims_6))

        dims_7 = ['lat', 'gyfdvtz', 'time']
        self.assertEqual(['lat', 'gyfdvtz', 'time'],
                         normalize_variable_dims_description(dims_7))

        dims_8 = ['gyfdvtz']
        self.assertEqual(['gyfdvtz'],
                         normalize_variable_dims_description(dims_8))
