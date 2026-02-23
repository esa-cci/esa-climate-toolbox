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


class CategoricalColorMap:

    def __init__(self, name: str, cmap_values: List[Number], cmap_colors: List[str], cmap_labels: List [str] = None):
        self._name = name
        if len(cmap_values) < 1:
            raise ValidationError("Categorical Color Map must contain at least one value.")
        num_cmap_labels = len(cmap_labels) if cmap_labels else 0
        cmap_labels = cmap_labels or [""] * len(cmap_values)
        if len({len(cmap_values), len(cmap_labels), len(cmap_colors)}) > 1:
            if num_cmap_labels > 0:
                raise ValidationError(
                    f"Number of color map values ({len(cmap_values)}), "
                    f"number of labels ({num_cmap_labels}) "
                    f"and number of colors ({len(cmap_colors)}) must be equal.")
            else:
                raise ValidationError(
                    f"Number of color map values ({len(cmap_values)}) "
                    f"and number of colors ({len(cmap_colors)}) must be equal.")
        self._values = np.array(cmap_values)
        self._labels = cmap_labels
        self._colors = cmap_colors
        self._register_color_map()

    @property
    def name(self) -> str:
        return self._name

    @property
    def values(self):
        return self._values

    @property
    def labels(self):
        return self._labels

    @property
    def colors(self):
        return self._colors

    def _register_color_map(self):
        num_entries = len(self._values)
        self._cmap = ListedColormap(
            self.colors, name=self.name, N=num_entries
        )
        if not self.name in matplotlib.colormaps:
            matplotlib.colormaps.register(cmap=self._cmap)

    @property
    def cmap(self):
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


class ColorMapRegistry:

    def __init__(self):
        self._cmaps = dict()

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
    def random_cmap_name():
        characters = string.ascii_lowercase + string.digits
        character_part = ''.join(random.choices(characters, k=6))
        return f"cmap_{character_part}"

    def register_categorical_color_map(
            self, cm_values: List, cm_name: str = None, cm_colors: List [str] = None, cm_labels: List[str] = None
    ) -> str:
        if cm_name is None:
            cm_name = self.random_cmap_name()
            while self.has_color_map(cm_name):
                cm_name = self.random_cmap_name()
        cm_entries = []
        for i, cm_value in enumerate(cm_values):
            cm_entry = dict()
            cm_entry["value"] = cm_value
            if cm_colors:
                cm_entry["color"] = cm_colors[i]
            if cm_labels:
                cm_entry["label"] = cm_labels[i]
            cm_entries.append(cm_entry)
        self._register_categorical_color_map(cm_name, cm_entries)
        return cm_name

    def deregister_color_map(self, cm_name: str):
        self._cmaps.pop(cm_name)

    def has_color_map(self, cm_name: str):
        return cm_name in self._cmaps

    def get_color_map(self, cm_name: str):
        return self._cmaps[cm_name]

    def _register_categorical_color_map(self, cm_name: str, cm_entries: List[Dict]):
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
        ccm = CategoricalColorMap(cm_name, values, colors, labels)
        self._cmaps[cm_name] = ccm


dir_path = os.path.dirname(os.path.abspath(__file__))
categorical_colormaps_file = os.path.join(dir_path, 'data/categorical_colormaps.yml')
with open(categorical_colormaps_file, "r") as cm:
    color_maps_dict = yaml.safe_load(cm)

COLOR_MAP_REGISTRY = ColorMapRegistry()

def ensure_cmaps_loaded():
    for cm_name, cm_entries in color_maps_dict.items():
        if not COLOR_MAP_REGISTRY.has_color_map(cm_name):
            COLOR_MAP_REGISTRY._register_categorical_color_map(cm_name, cm_entries)

ensure_cmaps_loaded()
