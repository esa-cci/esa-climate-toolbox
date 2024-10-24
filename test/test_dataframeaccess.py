import unittest

from xcube_cci.ccicdc import CciCdc
from xcube_cci.dataframeaccess import DataFrameAccessor

GHG_DS_ID = "esacci.GHG.satellite-orbit-frequency.L2.CH4.SCIAMACHY.Envisat.IMAP.v7-2.r1"


class DataFrameAccessTest(unittest.TestCase):

    def setUp(self) -> None:
        ccicdc = CciCdc(data_type='geodataframe')
        self._dfa = DataFrameAccessor(ccicdc, GHG_DS_ID, {})

    def test_get_geodataframe(self):
        gdf = self._dfa.get_geodataframe()
        self.assertIsNotNone(gdf)

    def test_get_geodataframe_for_dataset(self):
        gdf = self._dfa._get_features_from_cci_cdc(0)
        self.assertIsNotNone(gdf)
