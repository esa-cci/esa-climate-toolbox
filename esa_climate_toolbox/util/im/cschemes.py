# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative
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

from abc import ABC
from abc import abstractmethod
import colorsys
import logging
import matplotlib
from matplotlib.colors import BoundaryNorm
from matplotlib.colors import ListedColormap
import numpy as np
import os
import random
import string
import yaml
from typing import Dict
from typing import List

from esa_climate_toolbox.core.types import ValidationError
from xcube.core.gridmapping.helpers import Number

_LOG = logging.getLogger('ect')

LAND_COVER_CCI_CMAP = 'land_cover_cci'
LAND_COVER_FIRE_CCI_MAP = "land_cover_fire_cci"
HIGH_RES_LAND_COVER_CCI_CMAP = 'highres_land_cover_cci'


class ColorScheme(ABC):

    def __init__(
            self, color_scheme_name: str, color_map_name: str,
            cmap_values: np.array, cmap_colors: List[str], cmap_labels: List [str]
    ):
        self._color_scheme_name = color_scheme_name
        self._color_map_name = color_map_name
        self._values = cmap_values
        self._labels = cmap_labels
        self._colors = cmap_colors
        self._cmap = None
        self._register_color_map()

    @property
    def color_scheme_name(self) -> str:
        return self._color_scheme_name

    @property
    def color_map_name(self) -> str:
        return self._color_map_name

    @property
    def values(self):
        return self._values

    @property
    def labels(self):
        return self._labels

    @property
    def colors(self):
        return self._colors

    @property
    @abstractmethod
    def cmap(self):
        pass

    def _register_color_map(self):
        if not self.color_map_name in matplotlib.colormaps:
            matplotlib.colormaps.register(cmap=self.cmap)

    @property
    @abstractmethod
    def norm(self):
        pass

    @property
    @abstractmethod
    def tick_values(self):
        pass


class CategoricalColorScheme(ColorScheme):

    def __init__(
            self, color_scheme_name: str, color_map_name: str, cscheme_values: List[Number],
            cmap_colors: List[str], cscheme_labels: List[str] = None
    ):
        if len(cscheme_values) < 1:
            raise ValidationError("Categorical Color Scheme must contain at least one value.")
        num_cscheme_labels = len(cscheme_labels) if cscheme_labels else 0
        cscheme_labels = cscheme_labels or [""] * len(cscheme_values)
        if len({len(cscheme_values), len(cscheme_labels), len(cmap_colors)}) > 1:
            if num_cscheme_labels > 0:
                raise ValidationError(
                    f"Number of color scheme values ({len(cscheme_values)}), "
                    f"number of labels ({num_cscheme_labels}) "
                    f"and number of colors ({len(cmap_colors)}) must be equal.")
            else:
                raise ValidationError(
                    f"Number of color scheme values ({len(cscheme_values)}) "
                    f"and number of colors ({len(cmap_colors)}) must be equal.")
        super().__init__(color_scheme_name, color_map_name, np.array(cscheme_values), cmap_colors, cscheme_labels)
        self._register_color_map()

    @property
    def cmap(self):
        if self._cmap is None:
            num_entries = len(self._values)
            self._cmap = ListedColormap(
                self.colors, name=self.color_map_name, N=num_entries
            )
        return self._cmap

    def _color_bounds(self):
        color_bounds = np.zeros(len(self.values) + 1, dtype=float)
        color_bounds[1:-1] = (self.values[:-1] + self.values[1:]) / 2
        color_bounds[0] = self.values[0] - 0.5
        color_bounds[-1] = self.values[-1] + 0.5
        return color_bounds

    @property
    def norm(self):
        color_bounds = self._color_bounds()
        return BoundaryNorm(color_bounds, len(self.values))

    @property
    def tick_values(self):
        color_bounds = self._color_bounds()
        return (color_bounds[:-1] + color_bounds[1:]) / 2


class ColorSchemeRegistry:

    def __init__(self):
        self._colorschemes = dict()

    @staticmethod
    def _random_color():
        hue = random.random()
        saturation = random.uniform(0.7, 1.0)
        value = random.uniform(0.7, 1.0)
        r, g, b = colorsys.hsv_to_rgb(hue, saturation, value)
        return "#{:02X}{:02X}{:02X}".format(
            int(r * 255),
            int(g * 255),
            int(b * 255)
        )

    @staticmethod
    def random_part():
        characters = string.ascii_lowercase + string.digits
        return ''.join(random.choices(characters, k=6))

    def random_cmap_name(self):
        return f"cmap_{self.random_part()}"

    def random_cscheme_name(self):
        return f"cscheme_{self.random_part()}"

    def register_categorical_color_scheme(
            self, cm_values: List, color_scheme_name: str = None, color_map_name: str = None,
            cm_colors: List [str] = None, cm_labels: List[str] = None
    ) -> str:
        if color_scheme_name is None:
            color_scheme_name = self.random_cscheme_name()
            while self.has_color_scheme(color_scheme_name):
                color_scheme_name = self.random_cscheme_name()
        if color_map_name is None:
            color_map_name = self.random_cmap_name()
            while color_map_name in matplotlib.colormaps:
                color_map_name = self.random_cmap_name()
        cm_entries = []
        for i, cm_value in enumerate(cm_values):
            cm_entry = dict()
            cm_entry["value"] = cm_value
            if cm_colors:
                cm_entry["color"] = cm_colors[i]
            if cm_labels:
                cm_entry["label"] = cm_labels[i]
            cm_entries.append(cm_entry)
        self._register_categorical_color_scheme(color_scheme_name, color_map_name, cm_entries)
        return color_scheme_name

    def deregister_color_scheme(self, cs_name: str):
        self._colorschemes.pop(cs_name)

    def has_color_scheme(self, cs_name: str):
        return cs_name in self._colorschemes

    def get_color_scheme(self, cs_name: str) -> ColorScheme:
        return self._colorschemes[cs_name]

    def get_color_scheme_names(self):
        return list(self._colorschemes.keys())

    def _register_categorical_color_scheme(
            self, color_scheme_name: str, color_map_name: str, cm_entries: List[Dict]
    ):
        values = []
        labels = []
        colors = []
        for cm_entry in cm_entries:
            values.append(cm_entry.get("value"))
            labels.append(cm_entry.get("label", ""))
            if "color" in cm_entry:
                colors.append(cm_entry.get("color"))
            else:
                colors.append(self._random_color())
        ccs = CategoricalColorScheme(color_scheme_name, color_map_name, values, colors, labels)
        self._colorschemes[color_scheme_name] = ccs

COLOR_SCHEME_REGISTRY = ColorSchemeRegistry()

def ensure_cschemes_loaded():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    categorical_colorschemes_file = os.path.join(dir_path, 'data/categorical_colorschemes.yml')
    with open(categorical_colorschemes_file, "r") as cs:
        color_schemes_dict = yaml.safe_load(cs)
    for cs_name, cs_values in color_schemes_dict.items():
        if not COLOR_SCHEME_REGISTRY.has_color_scheme(cs_name):
            cm_name = cs_values["color_map_name"]
            cs_entries = cs_values["entries"]
            COLOR_SCHEME_REGISTRY._register_categorical_color_scheme(cs_name, cm_name, cs_entries)

ensure_cschemes_loaded()
