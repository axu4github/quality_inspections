# -*- coding: utf-8 -*-

import unittest
from quality_inspections import search_by_solr


class TestQualityInspections(unittest.TestCase):

    def test_function_search_by_solr(self):
        """ 测试 search_by_solr 方法 """
        solr_condtitions = "start_time: {st} AND area_of_job: {aoj}".format(
            st="[1427472000000 TO 1427558399000]", aoj="dy-gz")
        docs = search_by_solr(solr_condtitions)

        self.assertTrue(len(docs) != 0)
        self.assertTrue("id" in docs[0])


if __name__ == "__main__":
    unittest.main()
