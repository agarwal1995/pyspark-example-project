"""
Compare2Dataframe.py
~~~~~~~~
"""
import time

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
import datacompy as dc

from dependencies.spark import start_spark

# /home/raag/spark/bin/spark-submit --master local[*] --py-files packages.zip --files configs/etl_config.json jobs/Compare2Dataframe.py

def main():
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    path1='/home/raag/Study/github/pyspark-example-project/comparing/test1.csv'
    path2='/home/raag/Study/github/pyspark-example-project/comparing/test2.csv'
    df1 = extract_data(spark, path1)
    df2 = extract_data(spark, path2)

    # df1.show()
    # df2.show()

    cols = df1.columns
    # df3 = df1.join(df2, cols, 'left_anti').withColumn("X", lit('ab'))
    # df3.show()
    #
    # df4 = df2.join(df1, cols, 'left_anti').withColumn("X", lit('ab'))
    # df4.show()


    # cols = ['name','installation']
    #
    # ts1 = time.time()
    # log.info("---------------------------  1 = " + str(ts1))
    checkWithExcept(df1, df2, cols, log)
    # ts2 = time.time()- ts1
    # log.info("---------------------------  2 = " + str(ts2))
    #
    #
    # ts1 = time.time()
    # log.info("---------------------------  3 = " + str(ts1))
    # checkWithJoin(df1, df2, cols, log)
    # ts2 = time.time()- ts1
    # log.info("---------------------------  4 = " + str(ts2))
    #
    # ts1 = time.time()
    # log.info("---------------------------  5 = " + str(ts1))
    # checkWithALL(df1, df2, cols, log)
    # ts2 = time.time()- ts1
    # log.info("---------------------------  6 = " + str(ts2))ts2


    # df1.show()
    # df2.show()
    #
    #
    #
    # df1.printSchema()
    #

    # df_join_1 = df1.join(df2, df1.columns , "left_anti")
    # df_join_2 = df2.join(df1, df2.columns , "left_anti")
    #
    # df3 = df_join_1.union(df_join_2)


    # df3.show()
    #
    # cols_to_exclude = ["station_id", "name"]
    # result_list = [col_name for col_name in df1.columns if col_name not in cols_to_exclude]
    #
    # df_join_3 = df1.join(df2, result_list, "left_anti")
    # df_join_4 = df2.join(df1, result_list, "left_anti")
    # df3_2 = df_join_3.union(df_join_4)
    # df3_2.show()

    # df4 = df1.exceptAll(df2)
    # df5 = df2.exceptAll(df1)
    # df4.show()
    # df5.show()
    #
    # df6 = df4.unionAll(df5)
    # df6.show()



    # df7 = df1.select(['station_id','name','installation']).subtract(df2.select(['station_id','name','installation_x']))
    # df8 = df2.select(['station_id','name','installation_x']).subtract(df1.select(['station_id','name','installation']))
    # df7 = df1.subtract(df2)
    # df8 = df2.subtract(df1)
    # df7.show()
    # df8.show()
    # df9 = df7.unionAll(df8)
    # df9.show()
    # common_keys=[('station_id','station_id'),('name','name'),('lat','lat'),('long','long'),('dockcount','dockcount'),('landmark','landmark'),('installation','installation')]
    # comparison = dc.Compare(spark, df1, df2, join_columns=['station_id'])
    # report_file=    comparison.report()
    # report(file=<_io.TextIOWrapper name='<stdout>' mode='w' encoding='utf-8'>)
    #
    # x = comparison.rows_both_all()
    # with open('my_report.txt', 'w') as report_file:
    #     comparison.report(file=report_file)

    # comparison.rows_both_mismatch.display()

def method(df):
    df.show()

def checkWithExcept(df1, df2, cols, log):
    df1 = df1.select(cols)
    df2 = df2.select(cols)
    df3 = df1.subtract(df2)
    df3.show()
    log.info("=======================================" + str(1) + "ea " +str(df3.count()))
    return None

def checkWithJoin(df1, df2, cols, log):
    df3 = df1.join(df2, cols, "left_anti")
    # df3.show()
    log.info("=======================================" + str(2) + "ea " +str(df3.count()))
    return None

def checkWithALL(df1, df2, cols, log):
    df3 = df1.exceptAll(df2)
    # df3.show()
    log.info("=======================================" + str(2) + "ea " +str(df3.count()))
    return None

def extract_data(spark, path):
    """Load data from Parquet file format.

    :param path:
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .option("header","true")
        .csv(path))

    return df

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()