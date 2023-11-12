from unittest import TestCase

from esa_climate_toolbox.util.undefined import UNDEFINED


class UndefinedTest(TestCase):

    def test_undefined(self):
        self.assertIsNotNone(UNDEFINED)
        self.assertEqual(str(UNDEFINED), 'UNDEFINED')
        self.assertEqual(repr(UNDEFINED), 'UNDEFINED')
