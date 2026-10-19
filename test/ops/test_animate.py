"""
Tests for animation operations
"""
import itertools
import os
import sys
import shutil
import tempfile
from contextlib import contextmanager
from unittest import TestCase
import unittest

import xarray as xr
import numpy as np
import pandas as pd

from esa_climate_toolbox.ops.animate import animate_map
from esa_climate_toolbox.ops import export_animated_gif

_counter = itertools.count()
ON_WIN = sys.platform == 'win32'


@contextmanager
def create_tmp_file(name, ext):
    """
    Create a temporary filename for testing
    """
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, '{}{}.{}'.format(name, next(_counter), ext))
    try:
        yield path
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError:
            if not ON_WIN:
                raise


@unittest.skipIf(condition=os.environ.get('ECT_DISABLE_PLOT_TESTS', None),
                 reason="skipped if ECT_DISABLE_PLOT_TESTS=1")
class TestExportAnimatedGif(TestCase):
    def test_export_animated_gif(self):
        values = np.arange(36, dtype=float).reshape(3, 3, 4)
        values[0, 0, 0] = np.nan
        values[1, 0, 0] = np.inf
        values[2, :, :] = np.nan
        dataset = xr.Dataset(
            {'thickness': (('time', 'lat', 'lon'), values,
                           {'long_name': 'Ice thickness', 'units': 'm'})},
            coords={'time': pd.date_range('2020-01-01', periods=3),
                    'lat': [50., 51., 52.], 'lon': [0., 1., 2., 3.]},
        )
        with create_tmp_file('animation', 'gif') as path:
            result = export_animated_gif(dataset, var='thickness', file=path)
            self.assertEqual(path, result)
            self.assertTrue(os.path.isfile(path))
            self.assertGreater(os.path.getsize(path), 0)

    def test_export_projected_dask_cube(self):
        import dask.array as da

        dataset = xr.Dataset(
            {'temperature': (('yc', 'time', 'xc'),
                             da.from_array(np.arange(24).reshape(3, 2, 4),
                                           chunks=(3, 1, 4)))},
            coords={'time': pd.date_range('2020-01-01', periods=2),
                    'yc': [25., 50., 75.], 'xc': [0., 25., 50., 75.]},
        )
        with create_tmp_file('projected', 'gif') as path:
            result = export_animated_gif(dataset, var='temperature', file=path,
                                         vmin=0., vmax=24., interval=1000)
            self.assertEqual(path, result)
            self.assertTrue(os.path.isfile(path))
            self.assertGreater(os.path.getsize(path), 0)



@unittest.skipIf(condition=os.environ.get('ECT_DISABLE_PLOT_TESTS', None),
                 reason="skipped if ECT_DISABLE_PLOT_TESTS=1")
class TestAnimateMap(TestCase):
    """
    Test animate_map() operation
    """

    def test_animate_map(self):
        # Test the nominal functionality. This doesn't check that the animation
        # is what is expected, rather, it simply tests if it seems to have been created
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test if extents can be used
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        region='-40.0, -20.0, 50.0, 60.0',
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test setting a Title
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        title='Title',
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test using a global color map
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        true_range=True,
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_plot_map_exceptions(self):
        # Test if the corner cases are detected without creating a plot for it.

        # Test value error is raised when passing an unexpected dataset type
        with create_tmp_file('remove_me', 'html') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                animate_map([1, 2, 4], file=tmp_file)
            self.assertEqual(str(cm.exception),
                             "Input 'ds' for operation "
                             "'esa_climate_toolbox.ops.animate.animate_map' "
                             "must be of type 'Dataset', but got type 'list'.")
            self.assertFalse(os.path.isfile(tmp_file))

        # Test the extensions bound checking
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(2, 4, 1)),
            'lat': np.linspace(-89.5, 89.5, 2),
            'lon': np.linspace(-179.5, 179.5, 4)})

        with self.assertRaises(ValueError):
            region = '-40.0, -95.0, 50.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, 95.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-181.0, -20.0, 50.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 181.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, -25.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-20.0, -20.0, -25.0, 60.0',
            animate_map(dataset, region=region)

        # Test all-nan dataset
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.full([5, 10, 2], np.nan)),
            'second': (['lat', 'lon', 'time'], np.full([5, 10, 2], np.nan)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'html') as tmp_file:
            with self.assertRaises(ValueError):
                animate_map(dataset,
                            var='second',
                            true_range=True,
                            file=tmp_file)
