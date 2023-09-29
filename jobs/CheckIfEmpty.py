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

    # path2='/home/raag/Study/github/pyspark-example-project/comparing/test.json'
    path2='/home/raag/Study/github/pyspark-example-project/comparing/station_data_4.csv'
    df2 = extract_data(spark, path2)
    ts1 = time.time()
    log.info("============== " + str(df2.count()))
    ts2 = time.time() - ts1
    log.info("==============2 " + str(ts2))

    ts1 = time.time()
    res = len(df2.head(1))
    log.info("==============3 " + str(res))
    ts2 = time.time() - ts1
    log.info("==============4 " + str(ts2))
    
    rows = []
    
    rows.append(Row(jobID='abc', status='bhu',run='abc'))
    rows.append(Row(jobID='ac', status='b2u',run='abc'))
    df = spark.createDataFrame(rows)
    df.show()
    
    if len(df2.head(1)) == 0:
        log.info("mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmMMMMMMMMmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm")
    else:
        log.info('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqQQQQQQQQQQQQQQQQQQQQQQQqqqqqqqqqqqqqqqqqqqqqqq')
        
    errors = {}
    errors['abc']  = 'ui'
    errors['abd']  = 'ui2'
    errors['abt']  = 'ui3'
    
    log.info(errors['abc'])
    log.info(errors['abd'])
    log.info(errors.get('abc'))
    log.info(errors.get('ac') is None)
    log.info(errors.get('abc') is None)

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