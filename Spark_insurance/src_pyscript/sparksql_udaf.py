#!/usr/bin/env python
# @desc
__coding__ = "utf-8"
__author__ = "baidu team"

import os
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

# 指定spark的安装目录
os.environ['SPARK_HOME'] = '/export/server/spark'
# 指定python3解释器所在的地方
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("使用自定义的UDAF函数实现纵向迭代")

    # 1、创建SparkSession对象
    spark = SparkSession.builder \
        .config('spark.sql.shuffle.partitions', 1) \
        .appName('sparksql_udaf') \
        .master('local[*]') \
        .getOrCreate()

    # 2、数据输入
    spark.sql("""
        create or replace temporary view t01(c1,c2,c3) as
        values
               (1,1,6),
               (1,2,23),
               (1,3,8),
               (1,4,4),
               (1,5,10),
               (2,1,23),
               (2,2,14),
               (2,3,17),
               (2,4,20);
    """)

    # 3、数据处理
    # 3.1 当c2 = 1, 则c4 = 1
    spark.sql("""
        create or replace temporary view t02 as
        select
            c1, c2, c3,
            if (c2=1, 1, null) as c4
        from t01;
    """)

    # 3.2 否则 c4 = (上一个c4 +  当前的c3)/2。需要使用lag
    """
        基于Pandas的UDAF函数，对输入的参数类型和返回的数据类型的要求如下：
            输入的参数类型：Pandas中的Series对象
            返回的数据类型：标量，Python中的基本数据类型
            
        基于Pandas的UDF函数，对输入的参数类型和返回的数据类型的要求如下：
            输入的参数类型：Pandas中的Series对象
            返回的数据类型：Pandas中的Series对象
    """


    # 3.2.1 创建自定义的Python函数，这里的逻辑根据业务需求进行编写
    # @F.pandas_udf(returnType=FloatType())
    @F.pandas_udf(returnType='float')
    def udaf_c4(c3: pd.Series, c4: pd.Series) -> float:
        tmp_c4 = 0  # 1 -> 7.5 -> 12.25

        # 即使经过UDAF函数的处理，但是每次传入进来的c4的值没有发生变化。因此每次得从头开始遍历计算每行c4的值
        # print(c3)
        # print("-"*10)
        # print(c4)
        # print("-" * 10)

        for i in range(0, len(c3)):
            if i == 0:
                tmp_c4 = c4[0]
            else:
                tmp_c4 = (c3[i] + tmp_c4) / 2

            print(f'{i}-----{tmp_c4}')

        return tmp_c4


    # 3.2.2 将自定义的Python函数注册到SparkSQL中
    spark.udf.register('sql_udaf_c4', udaf_c4)

    # 3.2.3 调用
    spark.sql("""
        select
            c1,c2,c3,
            -- if(c2=1,1,(lag(c4) over(partition by c1 order by c2) + c3)/2) as c4
            sql_udaf_c4(c3,c4) over(partition by c1 order by c2) as c4
        from t02;
    """).show()

    # 4、释放资源
    spark.stop()
