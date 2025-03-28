import unittest

import numpy as np
from numpy.testing import assert_almost_equal

import esa_climate_toolbox.ops.gapfilling as gf

GAP = np.nan

KERNEL = np.ones((3, 3), dtype=np.float64)


class FillgapsLowpass2d(unittest.TestCase):
    def _test_fillgaps(self, src, desired_out, desired_gaps_filled):
        src = np.array(src)
        gc1 = gf.count_gaps(src)
        actual_out = gf.fillgaps_lowpass_2d(src, kernel=KERNEL)
        gc2 = gf.count_gaps(actual_out)
        actual_gaps_filled = gc1 - gc2
        assert_almost_equal(actual_out, np.array(desired_out))
        self.assertEqual(actual_gaps_filled, desired_gaps_filled)

    def test_0_missing(self):
        self._test_fillgaps([[1.0, 2.0], [3.0, 4.0]], [[1.0, 2.0], [3.0, 4.0]], 0)

    def test_1_missing(self):
        self._test_fillgaps([[GAP]], [[GAP]], 0)

        _F_ = (2 + 3 + 4) / 3.0
        self._test_fillgaps([[GAP, 2.0], [3.0, 4.0]], [[_F_, 2.0], [3.0, 4.0]], 1)

        _F_ = (1 + 2 + 3) / 3.0
        self._test_fillgaps([[1.0, 2.0], [3.0, GAP]], [[1.0, 2.0], [3.0, _F_]], 1)

        _F_ = (1 + 2 + 3 + 4 + 6 + 7 + 8 + 9) / 8.0
        self._test_fillgaps(
            [[1.0, 2.0, 3.0], [4.0, GAP, 6.0], [7.0, 8.0, 9.0]],
            [[1.0, 2.0, 3.0], [4.0, _F_, 6.0], [7.0, 8.0, 9.0]],
            1,
        )

    def test_2_missing(self):
        self._test_fillgaps([[GAP, GAP]], [[GAP, GAP]], 0)

        F1_ = (2 + 3) / 2.0
        F2_ = (2 + 3) / 2.0
        self._test_fillgaps([[GAP, 2.0], [3.0, GAP]], [[F1_, 2.0], [3.0, F2_]], 2)

        F1_ = (2 + 4) / 2.0
        F2_ = (2 + 3 + 4 + 6 + 7 + 8 + 9) / 7.0
        self._test_fillgaps(
            [[GAP, 2.0, 3.0], [4.0, GAP, 6.0], [7.0, 8.0, 9.0]],
            [[F1_, 2.0, 3.0], [4.0, F2_, 6.0], [7.0, 8.0, 9.0]],
            2,
        )

    def test_3_missing(self):
        self._test_fillgaps([[GAP, GAP], [GAP, 4.0]], [[4.0, 4.0], [4.0, 4.0]], 3)

        F1_ = 2.0
        F2_ = (2 + 7 + 8) / 3.0
        F3_ = (2 + 3 + 6 + 7 + 8 + 9) / 6.0
        self._test_fillgaps(
            [[GAP, 2.0, 3.0], [GAP, GAP, 6.0], [7.0, 8.0, 9.0]],
            [[F1_, 2.0, 3.0], [F2_, F3_, 6.0], [7.0, 8.0, 9.0]],
            3,
        )

    def test_4_missing(self):
        self._test_fillgaps([[GAP, GAP], [GAP, GAP]], [[GAP, GAP], [GAP, GAP]], 0)

        F1_ = (3 + 6) / 2.0
        F2_ = (7 + 8) / 2.0
        F3_ = (3 + 6 + 7 + 8 + 9) / 5.0
        F4_ = (F1_ + F2_ + F3_) / 3.0
        self._test_fillgaps(
            [[GAP, GAP, 3.0], [GAP, GAP, 6.0], [7.0, 8.0, 9.0]],
            [[F4_, F1_, 3.0], [F2_, F3_, 6.0], [7.0, 8.0, 9.0]],
            4,
        )

    def test_9_missing(self):
        F1_ = (1 + 2 + 3 + 4 + 5 + 9) / 6.0
        F2_ = (2 + 3 + 4) / 3.0
        F3_ = (3 + 4) / 2.0
        F4_ = (5 + 9 + 13) / 3.0
        F5_ = (9 + 13) / 2.0
        F6_ = (F1_ + F2_ + F3_ + F4_ + F5_) / 5.0
        F7_ = (F2_ + F3_) / 2.0
        F8_ = (F4_ + F5_) / 2.0
        F9_ = (F6_ + F7_ + F8_) / 3.0
        self._test_fillgaps(
            [
                [1.0, 2.0, 3.0, 4.0],
                [5.0, GAP, GAP, GAP],
                [9.0, GAP, GAP, GAP],
                [13.0, GAP, GAP, GAP],
            ],
            [
                [1.0, 2.0, 3.0, 4.0],
                [5.0, F1_, F2_, F3_],
                [9.0, F4_, F6_, F7_],
                [13.0, F5_, F8_, F9_],
            ],
            9,
        )
