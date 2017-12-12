# -*- coding: utf-8 -*-

import unittest
from quality_inspections import (
    search_by_solr,
    get_metas
)


class TestQualityInspections(unittest.TestCase):

    def setUp(self):
        self.solr_condtition = "start_time: {} AND area_of_job: {}".format(
            "[1427472000000 TO 1427558399000]", "dy-gz")
        self.ids = ["dy-gz-t75474539_20150328_6353815.mp3"]

    def test_function_search_by_solr(self):
        """ 测试 search_by_solr 方法 """
        docs = search_by_solr(self.solr_condtition)

        self.assertTrue(len(docs) != 0)
        self.assertTrue("id" not in docs[0])

    def test_function_get_metas(self):
        """ 测试 get_metas 方法 """
        metas = get_metas(self.ids)

        self.assertEqual(len(metas), 1)
        self.assertTrue("cf:callnumber" in metas[0])
        self.assertTrue("cf:filename" in metas[0])
        self.assertTrue("cf:plaintextb" in metas[0])

    def test_function_get_metas_only_callnumber(self):
        """ hbase 只获取 callnumber """
        metas_of_callnumber = get_metas(self.ids, columns=("cf:callnumber",))

        self.assertEqual(len(metas_of_callnumber), 1)
        self.assertTrue("cf:callnumber" in metas_of_callnumber[0])
        self.assertTrue("cf:filename" not in metas_of_callnumber[0])
        self.assertTrue("cf:plaintextb" not in metas_of_callnumber[0])


if __name__ == "__main__":
    unittest.main()
