"""
    Download the data file from the above location and make it accessible to Spark.
    Source: https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv
    Task1: Show first 5 rows in RDD
"""

from pyspark.sql import SparkSession
import time
import logging
import sys


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
    rdd = spark.sparkContext.textFile(path)
    flat_rdd = rdd.flatMap(lambda x: x.split(',')).filter(lambda x: x != ' ')
    return flat_rdd


if __name__ == '__main__':
    start_time = time.time()
    log = logging.getLogger("TASK_1_1")
    logging.basicConfig(level=logging.INFO)

    file_path = "C:/Users/DSR/PycharmProjects/truata_code_challenge/input/groceries.csv"
    # Initiate spark session
    log.info(" TASK_1_1 Program Started")

    try:
        spark = spark_init("Truta: TASK_1_1")
        # Loading Source data
        src_rdd = read_data(spark, file_path)
        log.info(" TASK_1_1 : RDD 5 records as below")
        src_rdd.take(5)  # showing top 5 records
        log.info(" TASK_1_1 : RDD distinct count: {} ".format(src_rdd.distinct().count()))

    except Exception as e:
        log.info(" Exception: {}".format(str(e)))
        sys.exit(1)
    else:
        hours, rem = divmod(time.time() - start_time, 3600)
        mins, secs = divmod(rem, 60)
        log.info("*** TASK_1_1 Completed Successfully in : [{:0>2}hrs:{:0>2}mins:{:05.2f}secs ] ***"
                 .format(int(hours), int(mins), secs))
        sys.exit(0)
