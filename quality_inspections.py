# -*- coding: utf-8 -*-

from solrcloudpy.connection import SolrConnection
from solrcloudpy.parameters import SearchOptions
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

"""
基于Spark的并行质检处理

业务员流程：
1. 根据查询条件获取待质检数据集。
2. 根据质检规则对待质检的数据集进行质检。
3. 存储质检结果。
"""


def search_by_solr(solr_condtitions):
    """ 通过Solr完成检索 """
    SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
    SOLR_VERSION = "5.5.1"
    SOLR_TIMEOUT = 60000
    SOLR_COLLECTION = "collection1"
    SOLR_ROWS = 1000000

    conn = SolrConnection(
        SOLR_NODES, version=SOLR_VERSION, timeout=SOLR_TIMEOUT)
    coll = conn[SOLR_COLLECTION]

    se = SearchOptions()
    se.commonparams.q(solr_condtitions) \
                   .fl("id") \
                   .start(0) \
                   .rows(SOLR_ROWS)

    r = coll.search(se)
    docs = r.result.response.docs
    return docs


def fetch_prequality_dataset(condtitions=None):
    """ 根据查询条件获取带质检数据集 """
    prequality_dataset = []
    if condtitions is not None:
        prequality_dataset = search_by_solr(condtitions)

    return prequality_dataset


def quality_inspection(mini_dataset):
    """ 根据质检规则对待质检的数据集进行质检 """
    pass


def main():
    pass


if __name__ == "__main__":
    main()
