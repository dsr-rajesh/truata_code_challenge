"""
    Download the data file from the above location and make it accessible to Spark.
    Source: https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv
    Task3: Total Count of the products
"""

from pyspark.sql import SparkSession
import time
import logging
import sys
import os


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
    return flat_rdd.distinct().count()


def save_txt(cnt):
    """
       Save to desired output
    :param rdd:
    :return:
    """
    task_2b = open("C:/Users/DSR/PycharmProjects/truata_code_challenge/out/out_1_2b.txt", "wt")
    n = task_2b.write(cnt)
    task_2b.close()


if __name__ == '__main__':
    start_time = time.time()
    log = logging.getLogger("TASK_1_3")
    logging.basicConfig(level=logging.INFO)

    file_path = "C:/Users/DSR/PycharmProjects/truata_code_challenge/input/groceries.csv"
    # Initiate spark session
    log.info(" TASK_1_3 Program Started")
    try:
        spark = spark_init("Truta: TASK_1_3")

        # Loading Source data
        rdd_cnt = read_data(spark, file_path)
        rdd_cnt_str = "Count:  {}".format(rdd_cnt)
        # Save desired output
        save_txt(rdd_cnt_str)

        log.info(" TASK_1_3 : Files saved successfully")

    except Exception as e:
        log.info(" Exception: {}".format(str(e)))
        sys.exit(1)
    else:
        hours, rem = divmod(time.time() - start_time, 3600)
        mins, secs = divmod(rem, 60)
        log.info("*** TASK_1_3 Completed Successfully in : [{:0>2}hrs:{:0>2}mins:{:05.2f}secs ] ***"
                 .format(int(hours), int(mins), secs))
        sys.exit(0)
