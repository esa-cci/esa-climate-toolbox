from unittest import TestCase

from esa_climate_toolbox.core import plugin


class PluginTest(TestCase):

    def test_that_test_plugins_are_loaded(self):
        self.assertIsNotNone(plugin.PLUGIN_REGISTRY)

    def test_ect_init(self):
        # Yes, this is really a silly test :)
        # But this way we cover one more (empty) statement.
        plugin.ect_init(True, False, a=1, b=2)
