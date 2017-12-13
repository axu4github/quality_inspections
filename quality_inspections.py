# -*- coding: utf-8 -*-

from solrcloudpy.connection import SolrConnection
from solrcloudpy.parameters import SearchOptions
from pyspark.sql import SparkSession
import happybase
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

SOLR_NODES = ["10.0.1.27:8983", "10.0.1.28:8983"]
SOLR_VERSION = "5.5.1"
SOLR_TIMEOUT = 60000
SOLR_COLLECTION = "collection1"
SOLR_ROWS = 1000000

HBASE_HOST = "10.0.3.41"
HBASE_TABLE = "smartv"
HBASE_TIMEOUT = 60000

APP_NAME = "quality_inspection"


def group_by_num(l, n):
    """ 按照长度分组 """
    if n <= 0:
        return l

    return [l[i:i + n] for i in range(0, len(l), n)]


def search_by_solr(solr_condtitions):
    """ 通过Solr完成检索 """
    conn = SolrConnection(
        SOLR_NODES, version=SOLR_VERSION, timeout=SOLR_TIMEOUT)
    coll = conn[SOLR_COLLECTION]

    se = SearchOptions()
    se.commonparams.q(solr_condtitions) \
                   .fl("id") \
                   .start(0) \
                   .rows(SOLR_ROWS)

    docs = coll.search(se).result.response.docs
    return map(lambda doc: doc["id"], docs)


def get_metas(items, columns=None):
    """ 获取元数据信息 """
    hbase = happybase.Connection(HBASE_HOST, timeout=HBASE_TIMEOUT)
    table = hbase.table(HBASE_TABLE)
    return map(lambda item: table.row(item, columns=columns), items)


def fetch_prequality_dataset(condtitions=None):
    """ 根据查询条件获取带质检数据集 """
    prequality_dataset = []
    if condtitions is not None:
        prequality_dataset = search_by_solr(condtitions)

    return prequality_dataset


def quality_inspection(per_dataset):
    """ 根据质检规则对待质检的数据集进行质检 """
    pass


def _init_spark_session():
    return SparkSession.builder.appName(APP_NAME).getOrCreate()


def distributed_quality_inspection(prequality_dataset=None):
    """ 基于Spark的并行质检处理 """
    if prequality_dataset is not None:
        prequality_dataset = list(enumerate(prequality_dataset, start=1))
        spark = _init_spark_session()
        quality_results = spark.sparkContext \
                               .parallelize(prequality_dataset) \
                               .partitionBy(len(prequality_dataset)) \
                               .map(quality_inspection)
        quality_results.saveAsTextFile("")


def main():
    pass


if __name__ == "__main__":
    main()
