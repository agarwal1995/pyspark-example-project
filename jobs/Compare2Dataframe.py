"""
Compare2Dataframe.py
~~~~~~~~
"""


from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
import datacompy as dc

from dependencies.spark import start_spark



def main():
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    path1='/home/raag/Study/github/pyspark-example-project/comparing/station_data_1.csv'
    path2='/home/raag/Study/github/pyspark-example-project/comparing/station_data_2.csv'
    df1 = extract_data(spark, path1)
    df2 = extract_data(spark, path2)

    df1.show(100)
    df2.show(100)
    df1.printSchema()


    common_keys=[('station_id','station_id'),('name','name'),('lat','lat'),('long','long'),('dockcount','dockcount'),('landmark','landmark'),('installation','installation')]
    comparison = dc.SparkCompare(spark, df1, df2, join_columns=['station_id'])
    comparison.report()
    # comparison.rows_both_mismatch.display()
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