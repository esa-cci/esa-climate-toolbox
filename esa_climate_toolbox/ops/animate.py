# The MIT License (MIT)
# Copyright (c) 2024 ESA Climate Change Initiative
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

Data animation operations

Components
==========

## export_animated_gif

Exports a variable with time and two spatial dimensions to complete raster
plots in a GIF, with a shared color scale and explicit missing-value colors.

Usage in jupyter notebook:
```python
import esa_climate_toolbox.ops as ops
from esa_climate_toolbox.util.monitor import ConsoleMonitor

cube = ops.open_dataset('local.dataset_name')
ops.export_animated_gif(
    cube, var='my.var', file='my_var.gif',
        interval=1000, monitor=ConsoleMonitor(),
)
```

## animate_map

Animates a geospatial data slice on a world map.

General jupyter notebook usage:
```python
import esa_climate_toolbox.ops as ops
from IPython.core.display import display, HTML
dset = ops.open_dataset('local.dataset_name')
display(HTML(ops.animate_map(cc, var='var_name')))
```

If a file path is given, the plot is saved.
Supported formats: html

"""

import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import matplotlib
import matplotlib.animation as animation
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.colors import Colormap, to_rgba
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from PIL import Image

import cartopy.crs as ccrs
import xarray as xr
import numpy as np

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.types import DictLike
from esa_climate_toolbox.core.types import HTML
from esa_climate_toolbox.core.types import PolygonLike
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.core.types import VarName
from esa_climate_toolbox.util.monitor import Monitor

from esa_climate_toolbox.ops.plot_helpers import determine_cmap_params
from esa_climate_toolbox.ops.plot_helpers import get_var_data
from esa_climate_toolbox.ops.plot_helpers import handle_plot_polygon

matplotlib.use("Agg")

ANIMATION_FILE_FILTER = dict(name='Animation Outputs', extensions=['html', ])
GIF_FILE_FILTER = dict(name='Animated GIF', extensions=['gif'])


@op(tags=['plot', 'export'], version='1.0')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('file', file_open_mode='w', file_filters=[GIF_FILE_FILTER])
@op_input('interval', units='milliseconds', value_range=[10, 655350])
@op_input('loop', value_range=[0, 65535])
def export_animated_gif(ds: xr.Dataset,
                        var: VarName.TYPE,
                        file: str,
                        interval: int = 200,
                        loop: int = 0,
                        force: bool = False,
                        cmap: str = 'viridis',
                        missing_color: str = '#ff00ff',
                        vmin: float = None,
                        vmax: float = None,
                        title: str = None,
                        monitor: Monitor = Monitor.NONE) -> str:
    """Export a data cube variable as an animated GIF of complete raster plots.

    The variable must have exactly one ``time`` dimension and two spatial
    dimensions. The remaining dimensions, in their existing order, are used
    as y and x. Dimension names such as ``lat``/``lon`` and ``yc``/``xc`` are
    supported. Plots include coordinate axes, a colorbar, a time stamp and a
    separate legend entry for missing values. No map projection or coastlines
    are added, and no ancillary data are downloaded.

    All frames use the same color scale. Unless both limits are supplied,
    finite values are scanned one time slice at a time to determine the missing
    limits. NaN and positive/negative infinity use *missing_color*. A cube with
    no finite values uses a fallback range of 0 to 1; constant ranges are
    expanded automatically. A shared GIF palette keeps colors consistent.

    :param ds: Dataset containing the variable to export.
    :param var: Name of a real numeric variable with three dimensions.
    :param file: Target filename ending in ``.gif``. Its parent directory must
        exist. Existing files are overwritten only if *force* is True.
    :param interval: Frame duration in milliseconds, from 10 to 655350.
    :param loop: Number of repetitions, from 0 to 65535; 0 loops forever.
    :param force: Allow replacement of an existing regular file. Default False.
    :param cmap: Matplotlib colormap name. Default ``viridis``.
    :param missing_color: Opaque color for NaN and infinity, distinct from the
        data colormap. Default magenta (``#ff00ff``).
    :param vmin: Optional finite lower color scale limit.
    :param vmax: Optional finite upper color scale limit.
    :param title: Optional plot title. Defaults to the variable's long name or
        name. The current time coordinate is always appended.
    :param monitor: Progress and cancellation monitor.
    :return: Path of the written GIF as a string.
    :raises ValidationError: If the variable, dimensions, colors, limits or
        target filename are invalid.
    :raises FileExistsError: If the destination exists and *force* is False.
    :raises OSError: If the destination directory is missing or not writable,
        or writing the GIF fails.
    """
    path = Path(file)
    if path.suffix.lower() != '.gif':
        raise ValidationError('The target filename must end in .gif.')
    if not path.parent.is_dir():
        raise FileNotFoundError(f'Target directory does not exist: {path.parent}')
    if os.path.lexists(path):
        if not force:
            raise FileExistsError(f'Target already exists: {path}')
        if not path.is_file() or path.is_symlink():
            raise ValidationError('The target must be a regular file, not a directory or symlink.')
    if var not in ds.data_vars:
        raise ValidationError(f'Variable {var!r} does not exist in the dataset.')

    array = ds[var]
    if array.ndim != 3 or 'time' not in array.dims:
        raise ValidationError('The variable must have time and exactly two spatial dimensions.')
    if any(size == 0 for size in array.shape):
        raise ValidationError('The variable must not have empty dimensions.')
    if array.dtype.kind not in 'biuf':
        raise ValidationError('The variable must contain real numeric values.')
    if 'time' not in array.coords:
        raise ValidationError('The variable must have a time coordinate.')
    for name, limit in [('vmin', vmin), ('vmax', vmax)]:
        if limit is not None and not np.isfinite(limit):
            raise ValidationError(f'{name} must be finite.')
    if vmin is not None and vmax is not None and vmin >= vmax:
        raise ValidationError('vmin must be smaller than vmax.')

    try:
        colormap = matplotlib.colormaps[cmap].copy()
        bad_rgba = to_rgba(missing_color)
    except (KeyError, ValueError) as error:
        raise ValidationError(str(error)) from error
    if bad_rgba[3] != 1:
        raise ValidationError('missing_color must be opaque.')

    sampled_colors = colormap(np.linspace(0, 1, max(colormap.N, 239)))[:, :3]
    if np.any(np.all(np.isclose(sampled_colors, bad_rgba[:3], atol=1 / 255), axis=1)):
        raise ValidationError('missing_color must be distinct from the data colormap.')
    colormap = colormap.with_extremes(bad=bad_rgba)

    colors = np.vstack((colormap(np.linspace(0, 1, 239))[:, :3],
                        np.repeat(np.linspace(0, 1, 16)[:, None], 3, axis=1),
                        np.array(bad_rgba[:3])[None, :]))
    palette = Image.new('P', (1, 1))
    palette.putpalette(np.round(colors * 255).astype('uint8').ravel().tolist())
    figure = Figure(figsize=(8, 6), dpi=100, facecolor='white')
    canvas = FigureCanvasAgg(figure)
    temporary_path = None
    frames = None
    scan_range = vmin is None or vmax is None
    frame_count = array.sizes['time']
    try:
        with monitor.starting('Export animated GIF', frame_count * (2 if scan_range else 1) + 1):
            monitor.check_for_cancellation()
            if scan_range:
                data_min, data_max = np.inf, -np.inf
                for index in range(frame_count):
                    monitor.check_for_cancellation()
                    values = np.asarray(array.isel(time=index).values)
                    finite = values[np.isfinite(values)]
                    if finite.size:
                        data_min = min(data_min, float(finite.min()))
                        data_max = max(data_max, float(finite.max()))
                    monitor.progress(1, msg='Determine shared color scale')
                if data_min == np.inf:
                    data_min, data_max = 0., 1.
                vmin = data_min if vmin is None else vmin
                vmax = data_max if vmax is None else vmax
                if vmin > vmax:
                    raise ValidationError('The resulting vmin must not exceed vmax.')
                if vmin == vmax:
                    delta = max(abs(vmin) * 0.01, 0.5)
                    vmin, vmax = vmin - delta, vmax + delta

            axes = figure.subplots()
            figure.legend(handles=[Patch(facecolor=bad_rgba, label='Missing / non-finite')],
                          loc='lower center')
            frames = _render_gif_frames(array, figure, canvas, axes, palette,
                                        colormap, vmin, vmax, title, monitor)

            with tempfile.NamedTemporaryFile(dir=path.parent, suffix='.gif', delete=False) as output:
                temporary_path = Path(output.name)
                with next(frames) as first_frame:
                    first_frame.save(output, format='GIF', save_all=True,
                                     append_images=frames, optimize=False, disposal=2,
                                     duration=int(np.floor(interval / 10 + 0.5)) * 10,
                                     loop=loop)
            monitor.check_for_cancellation()
            if force:
                os.replace(temporary_path, path)
            else:
                with path.open('xb') as target:
                    try:
                        with temporary_path.open('rb') as source:
                            shutil.copyfileobj(source, target)
                    except BaseException:
                        target.close()
                        path.unlink()
                        raise
            monitor.progress(1, msg='GIF written')
    finally:
        if frames is not None:
            frames.close()
        figure.clear()
        palette.close()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return str(path)


def _render_gif_frames(array: xr.DataArray,
                       figure: Figure,
                       canvas: FigureCanvasAgg,
                       axes: Axes,
                       palette: Image.Image,
                       colormap: Colormap,
                       vmin: float,
                       vmax: float,
                       title: str | None,
                       monitor: Monitor) -> Generator[Image.Image, None, None]:
    """Yield rendered, indexed frames without retaining a list of RGB images."""
    image = None
    label = title if title is not None else array.attrs.get('long_name', array.name)
    for index in range(array.sizes['time']):
        monitor.check_for_cancellation()
        frame = array.isel(time=index, drop=True)
        values = np.asarray(frame.values)
        values = np.where(np.isfinite(values), values, np.nan)
        if image is None:
            image = frame.copy(data=values).plot.imshow(
                ax=axes, cmap=colormap, vmin=vmin, vmax=vmax,
                interpolation='nearest', add_colorbar=True,
            )
        else:
            image.set_data(np.ma.masked_invalid(values))
        time_value = array.time.values[index]
        timestamp = (np.datetime_as_string(time_value, unit='s')
                     if isinstance(time_value, np.datetime64) else str(time_value))
        axes.set_title(f'{label}\ntime = {timestamp}')
        if index == 0:
            figure.tight_layout(rect=(0, 0.06, 1, 1))
        canvas.draw()
        with Image.fromarray(np.asarray(canvas.buffer_rgba())) as rgba:
            with rgba.convert('RGB') as rgb:
                with rgb.quantize(palette=palette, dither=0) as indexed:
                    monitor.progress(1, msg=f'Render frame {index + 1}/{array.sizes["time"]}')
                    yield indexed


@op(tags=['plot'], res_pattern='animation_{index}')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('indexers', data_type=DictLike)
@op_input('region', data_type=PolygonLike)
@op_input('projection', value_set=[
    'PlateCarree', 'LambertCylindrical', 'Mercator', 'Miller', 'Mollweide',
    'Orthographic', 'Robinson', 'Sinusoidal', 'NorthPolarStereo', 'SouthPolarStereo']
          )
@op_input('central_lon', units='degrees', value_range=[-180, 180])
@op_input('title')
@op_input('cmap_params', data_type=DictLike)
@op_input('plot_properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[ANIMATION_FILE_FILTER])
def animate_map(ds: xr.Dataset,
                var: VarName.TYPE = None,
                animate_dim: str = 'time',
                interval: int = 200,
                true_range: bool = False,
                indexers: DictLike.TYPE = None,
                region: PolygonLike.TYPE = None,
                projection: str = 'PlateCarree',
                central_lon: float = 0.0,
                title: str = None,
                contour_plot: bool = False,
                cmap_params: DictLike.TYPE = None,
                plot_properties: DictLike.TYPE = None,
                file: str = None,
                monitor: Monitor = Monitor.NONE) -> HTML:
    """
    Create a geographic map animation for the variable given by dataset *ds*
    and variable name *var*.

    Creates an animation of the given variable from the given dataset on a map
    with coastal lines.
    In case no variable name is given, the first encountered variable in the
    dataset is animated.
    It is also possible to set extents of the animation. If no extents
    are given, a global animation is created.

    The following file formats for saving the animation are supported: html

    :param ds: the dataset containing the variable to animate
    :param var: the variable's name
    :param animate_dim: Dimension to animate, if none given defaults to time.
    :param interval: Delay between frames in milliseconds. Defaults to 200.
    :param true_range: If True, calculates colormap and colorbar configuration
        parameters from the whole dataset. Can potentially take a lot of time.
        Defaults to False, in which case the colormap is calculated from the first frame
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a
        dictionary or a comma-separated string of key-value pairs that maps the
        variable's dimension names to constant labels. e.g. "layer=4".
    :param region: Region to animate
    :param projection: name of a global projection,
        see http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html
    :param central_lon: central longitude of the projection in degrees
    :param title: an optional title
    :param contour_plot: If true plot a filled contour plot of data,
        otherwise plots a pixelated colormesh
    :param cmap_params: optional additional colormap configuration parameters,
           e.g. "vmax=300, cmap='magma'"
           For full reference refer to
           http://xarray.pydata.org/en/stable/generated/xarray.plot.contourf.html
    :param plot_properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5)"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/api/_as_gen/matplotlib.axes.Axes.contourf.html
    :param file: path to a file in which to save the animation
    :param monitor: A progress monitor.
    :return: An animation in HTML format
    """
    if not isinstance(ds, xr.Dataset):
        raise NotImplementedError('Only gridded datasets are currently supported')

    var_name = None
    if not var:
        for key in ds.data_vars.keys():
            var_name = key
            break
    else:
        var_name = VarName.convert(var)

    try:
        var = ds[var_name]
    except KeyError:
        raise ValidationError(
            'Provided variable name "{}" does not exist '
            'in the given dataset'.format(var_name)
        )

    indexers = DictLike.convert(indexers) or {}
    properties = DictLike.convert(plot_properties) or {}
    cmap_params = DictLike.convert(cmap_params) or {}

    extents = None
    bounds = handle_plot_polygon(region)
    if bounds:
        lon_min, lat_min, lon_max, lat_max = bounds
        extents = [lon_min, lon_max, lat_min, lat_max]

    if len(ds.lat) < 2 or len(ds.lon) < 2:
        # Matplotlib can not plot datasets with less than these dimensions with
        # contourf and pcolormesh methods
        raise ValidationError('The minimum dataset spatial dimensions to create a map'
                              ' plot are (2,2)')

    # See http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html#
    if projection == 'PlateCarree':
        proj = ccrs.PlateCarree(central_longitude=central_lon)
    elif projection == 'LambertCylindrical':
        proj = ccrs.LambertCylindrical(central_longitude=central_lon)
    elif projection == 'Mercator':
        proj = ccrs.Mercator(central_longitude=central_lon)
    elif projection == 'Miller':
        proj = ccrs.Miller(central_longitude=central_lon)
    elif projection == 'Mollweide':
        proj = ccrs.Mollweide(central_longitude=central_lon)
    elif projection == 'Orthographic':
        proj = ccrs.Orthographic(central_longitude=central_lon)
    elif projection == 'Robinson':
        proj = ccrs.Robinson(central_longitude=central_lon)
    elif projection == 'Sinusoidal':
        proj = ccrs.Sinusoidal(central_longitude=central_lon)
    elif projection == 'NorthPolarStereo':
        proj = ccrs.NorthPolarStereo(central_longitude=central_lon)
    elif projection == 'SouthPolarStereo':
        proj = ccrs.SouthPolarStereo(central_longitude=central_lon)
    else:
        raise ValidationError('illegal projection: "%s"' % projection)

    figure = plt.figure(figsize=(8, 4))
    ax = plt.axes(projection=proj)
    if extents:
        ax.set_extent(extents, ccrs.PlateCarree())
    else:
        ax.set_global()

    ax.coastlines()

    if not animate_dim:
        animate_dim = 'time'

    indexers[animate_dim] = var[animate_dim][0]

    var_data = get_var_data(var, indexers, remaining_dims=('lon', 'lat'))

    with monitor.starting("animate", len(var[animate_dim]) + 3):
        if true_range:
            data_min, data_max = _get_min_max(var, monitor=monitor)
        else:
            data_min, data_max = _get_min_max(var_data, monitor=monitor)

        cmap_params = determine_cmap_params(data_min, data_max, **cmap_params)
        plot_kwargs = {**properties, **cmap_params}

        # Plot the first frame to set-up the axes with the colorbar properly
        # transform keyword is for the coordinate our data is in, which in case of a
        # 'normal' lat/lon dataset is PlateCarree.
        if contour_plot:
            var_data.plot.contourf(
                ax=ax, transform=ccrs.PlateCarree(), add_colorbar=True, **plot_kwargs
            )
        else:
            var_data.plot.pcolormesh(
                ax=ax, transform=ccrs.PlateCarree(), add_colorbar=True, **plot_kwargs
            )
        if title:
            ax.set_title(title)
        figure.tight_layout()
        monitor.progress(1)

        def run(value):
            ax.clear()
            if extents:
                ax.set_extent(extents, ccrs.PlateCarree())
            else:
                ax.set_global()
            ax.coastlines()
            indexers[animate_dim] = value
            indexed_var_data = get_var_data(
                var, indexers, remaining_dims=('lon', 'lat')
            )
            indexed_var_data.plot.contourf(ax=ax, transform=ccrs.PlateCarree(),
                                           add_colorbar=False, **plot_kwargs)
            if title:
                ax.set_title(title)
            monitor.progress(1)
            return ax

        # Reports Optional[int] expected but iterable allowed
        # noinspection PyTypeChecker
        anim = animation.FuncAnimation(figure, run, [i for i in var[animate_dim]],
                                       interval=interval, blit=False, repeat=False)
        anim_html = anim.to_jshtml()

        # Prevent the animation for running after it's finished
        del anim

        # Delete the rogue temp-file
        try:
            os.remove('None0000000.png')
        except FileNotFoundError:
            pass

        if file:
            with open(file, 'w') as outfile:
                outfile.write(anim_html)
                monitor.progress(1)

    return HTML(anim_html)


def _get_min_max(data, monitor=None):
    """
    Get min and max of a dataset, while accounting for all-NaN
    datasets and observing it with the monitor.
    """
    with monitor.child(1).observing("find minimum"):
        data_min = data.min()
    if np.isnan(data_min):
        # Handle all-NaN dataset
        raise ValidationError(
            'Can not create an animation of a dataset containing only NaN values.'
        )
    else:
        with monitor.child(1).observing("find maximum"):
            data_max = data.max()

    return data_min.values, data_max.values
