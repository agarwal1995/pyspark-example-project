"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
import datacompy as dc

from dependencies.spark import start_spark


def main():
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # path1='/home/raag/Study/github/pyspark-example-project/comparing/station_data_1.csv'
    # path2='/home/raag/Study/github/pyspark-example-project/comparing/station_data_2.csv'
    # df1 = extract_data(spark, path1)
    # df2 = extract_data(spark, path2)
    # df_join_1 = df1.join(df2, df1.columns , "left_anti")
    # df_join_2 = df2.join(df1, df2.columns , "left_anti")
    # df3 = df_join_1.union(df_join_2)
    # df3.show()
    df = read_gcp_file(spark, "gs://raag/spark/binary_2.gz.parquet", config.get('path'), 'parquet')
    df.show()

def read_gcp_file(spark, data_path, key, file_type):
    # spark._jsc.hadoopConfiguaration().set("google.cloud.auth.service.account.json.keyfile", key)
    df = (spark.read.option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
          .option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS").option('spark.hadoop.google.cloud.auth.service.account.json.keyfile', key)
          .option("google.cloud.auth.service.account.enable", "true").option("header", "true"))
    if file_type == "json":
        df = df.option("multiline", "true").json(data_path)
    else:
        df = df.parquet(data_path)
    return df


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