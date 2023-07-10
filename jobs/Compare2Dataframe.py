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


    df3 = df1.join(df2, df1.installation == df2.installation_x, "left_anti")

    df3.show()

    df4 = df1.exceptAll(df2)
    df5 = df2.exceptAll(df1)
    df4.show()
    df5.show()

    df6 = df4.unionAll(df5)
    df6.show()



    df7 = df1.select(['station_id','name','installation']).subtract(df2.select(['station_id','name','installation_x']))
    df8 = df2.select(['station_id','name','installation_x']).subtract(df1.select(['station_id','name','installation']))
    df7.show()
    df8.show()
    df9 = df7.unionAll(df8)
    df9.show()
    # common_keys=[('station_id','station_id'),('name','name'),('lat','lat'),('long','long'),('dockcount','dockcount'),('landmark','landmark'),('installation','installation')]
    # comparison = dc.Compare(spark, df1, df2, join_columns=['station_id'])
    # report_file=    comparison.report()
    # report(file=<_io.TextIOWrapper name='<stdout>' mode='w' encoding='utf-8'>)
    #
    # x = comparison.rows_both_all()
    # with open('my_report.txt', 'w') as report_file:
    #     comparison.report(file=report_file)

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