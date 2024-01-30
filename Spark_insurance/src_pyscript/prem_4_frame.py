from _insurance_FIAA_main import executeSQLFile
import decimal
import pandas as pd
import spark as spark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DecimalType
import pyspark.sql.functions as F
import os

@F.pandas_udf(returnType=DecimalType(38, 7))
def pandas_func( qx: pd.Series) -> decimal:
    tmp_lx = 0
    for i in range(0, len(qx)):
        if i == 0:
            tmp_lx = 1
        else:
            tmp_lx = (tmp_lx * (1 - qx[i-1])).quantize(decimal.Decimal('0.0000000'))
    return tmp_lx

# 锁定远端操作环境, 避免存在多个版本环境的问题
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/bin/python"
if __name__ == '__main__':
    spark = SparkSession.builder.appName("FIAA_MAIN") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://node1:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    # 注册这个函数
    spark.udf.register('pandas_func', pandas_func)
    # 2- 读取SQL脚本, 执行SQL语句
    executeSQLFile('prem_4_ls.sql')
