import unittest

from xcube.util.extension import ExtensionRegistry
from esa_climate_toolbox.ds.dataaccess import CciCdcDataStore

from esa_climate_toolbox.plugin import init_plugin


class PluginTest(unittest.TestCase):

    def test_extensions_exist(self):
        ext_reg = ExtensionRegistry()

        init_plugin(ext_reg)

        self.assertTrue(ext_reg.has_extension('xcube.core.store', 'esa-cdc'))
        self.assertTrue(ext_reg.has_extension('xcube.core.store',
                                              'esa-climate-data-centre')
                        )

    def test_get_extension_short(self):
        ext_reg = ExtensionRegistry()

        init_plugin(ext_reg)

        store = ext_reg.get_component('xcube.core.store', 'esa-cdc')

        self.assertIsNotNone(store)
        self.assertIsInstance(store(), CciCdcDataStore)

    def test_get_extension_long(self):
        ext_reg = ExtensionRegistry()

        init_plugin(ext_reg)

        store = ext_reg.get_component('xcube.core.store',
                                      'esa-climate-data-centre'
                                      )
        self.assertIsNotNone(store)
        self.assertIsInstance(store(), CciCdcDataStore)
