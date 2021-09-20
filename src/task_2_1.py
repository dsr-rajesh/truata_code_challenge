"""
    Download the data file from the above location and make it accessible to Spark.
    Source: https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv
    Task1: Load data to DataFrame
"""
import time
import logging
import sys
from pyspark.sql import SparkSession


def spark_init(app_Name):
    """
      Spark session initialization
    :return: spark_session
    """
    return SparkSession.builder.appName(app_Name).enableHiveSupport().getOrCreate()


def read_data(spark, path):
    """
      Load file and return rdd
    :return:
    """
    df = spark.read.format("parquet").load(path)
    df.printSchema()
    return df


if __name__ == '__main__':
    start_time = time.time()
    log = logging.getLogger("TASK_2_1")
    logging.basicConfig(level=logging.INFO)

    file_path = "C:/Users/DSR/PycharmProjects/truata_code_challenge/input/airbnb.parquet"
    # Initiate spark session
    log.info(" TASK_2_1 Program Started")
    try:
        spark = spark_init("Truta: TASK_2_1")

        # Loading Source data
        src_df = read_data(spark, file_path)
        src_df.show(10,truncate=False)

        log.info(" TASK_2_1 : DF count-  {}".format(src_df.distinct.count()))

    except Exception as e:
        log.info(" Exception: {}".format(str(e)))
        sys.exit(1)
    else:
        hours, rem = divmod(time.time() - start_time, 3600)
        mins, secs = divmod(rem, 60)
        log.info("*** TASK_2_1 Completed Successfully in : [{:0>2}hrs:{:0>2}mins:{:05.2f}secs ] ***"
                 .format(int(hours), int(mins), secs))
        sys.exit(0)
