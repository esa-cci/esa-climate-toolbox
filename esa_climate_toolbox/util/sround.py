# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
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

from typing import Tuple
import math

# max number of significant digits for a 64-bit float
_MAX_SIGNIFICANT_DIGITS_AFTER_DOT = 15

_MIN_EXP = -323
_MAX_EXP = 308


def sround(value: float, num_digits: int = 0, int_part=False) -> float:
    """
    Round *value* to significant number of digits *num_digits*.

    :param value: The value to round.
    :param num_digits: The number of digits after the first significant digit.
    :param int_part:
    :return:
    """
    num_digits_extra = _num_digits_extra(value, int_part=int_part)
    num_digits += num_digits_extra
    return round(value, ndigits=num_digits)


def sround_range(range_value: Tuple[float, float],
                 num_digits: int = 0,
                 int_part=False) -> Tuple[float, float]:
    value_1, value_2 = range_value
    num_digits_extra_1 = _num_digits_extra(value_1, int_part=int_part)
    num_digits_extra_2 = _num_digits_extra(value_2, int_part=int_part)
    num_digits += min(num_digits_extra_1, num_digits_extra_2)
    return round(value_1, ndigits=num_digits), \
           round(value_2, ndigits=num_digits)


def _num_digits_extra(value: float, int_part: bool) -> int:
    num_digits = -int(math.floor(_limited_log10(value)))
    if num_digits < 0 and not int_part:
        return 0
    return num_digits


def _limited_log10(value: float) -> float:
    if value > 0.0:
        exp = math.log10(value)
    elif value < 0.0:
        exp = math.log10(-value)
    else:
        return _MIN_EXP

    if exp < _MIN_EXP:
        return _MIN_EXP
    if exp > _MAX_EXP:
        return _MAX_EXP

    return exp
