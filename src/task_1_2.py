"""
    Download the data file from the above location and make it accessible to Spark.
    Source: https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv
    Task2: min, max prices and row count
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


def products_prefix(word):
    return "Product :" + word


def read_transform(spark, path):
    """
      Load file and return rdd
    :return:
    """
    rdd = spark.sparkContext.textFile(path)
    flat_rdd = rdd.flatMap(lambda rec: rec.split(',')).filter(lambda rec: rec != ' ')
    dist_rdd = flat_rdd.distinct().map(lambda rec: products_prefix(rec))
    return dist_rdd


def save_txt(rdd):
    """
       Save to desired output
    :param rdd:
    :return:
    """
    rdd.coalesce(1).saveAsTextFile("C:/Users/DSR/PycharmProjects/truata_code_challenge/tmp/task_1")
    os.rename("C:/Users/DSR/PycharmProjects/truata_code_challenge/tmp/task_1/part-00000",
              "C:/Users/DSR/PycharmProjects/truata_code_challenge/out/out_1_2a.txt")


if __name__ == '__main__':
    start_time = time.time()
    log = logging.getLogger("TASK_1_2")
    logging.basicConfig(level=logging.INFO)

    file_path = "C:/Users/DSR/PycharmProjects/truata_code_challenge/input/groceries.csv"
    # Initiate spark session
    log.info(" TASK_1_2 Program Started")
    try:
        spark = spark_init("Truta: TASK_1_2")

        # Read and transform Source data
        rdd = read_transform(spark, file_path)

        # Save desired output
        save_txt(rdd)

        log.info(" TASK_1_2 : Files saved successfully")

    except Exception as e:
        log.info(" Exception: {}".format(str(e)))
        sys.exit(1)
    else:
        hours, rem = divmod(time.time() - start_time, 3600)
        mins, secs = divmod(rem, 60)
        log.info("*** TASK_1_2 Completed Successfully in : [{:0>2}hrs:{:0>2}mins:{:05.2f}secs ] ***"
                 .format(int(hours), int(mins), secs))
        sys.exit(0)
