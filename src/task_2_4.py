"""
    Download the data file from the above location and make it accessible to Spark.
    Source: https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv
    Task4: Aggregate operations on DF
"""
import time
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    df = spark.read.format("parquet").load(path)
    df.printSchema()
    return df


def save_txt(dest_df):
    """
       Save to desired output
    :param rdd:
    :return:
    """
    dest_df.coalesce(1).write.format("csv").option("header", "true").mode("append").save(
        "C:/Users/DSR/PycharmProjects/truata_code_challenge/tmp/task_4")
    os.rename(
        "C:/Users/DSR/PycharmProjects/truata_code_challenge/tmp/task_4/part-00000-5643c768-e709-4c0c-9fc3-bf99626f3142-c000.csv",
        "C:/Users/DSR/PycharmProjects/truata_code_challenge/out/out_2_4.csv")


if __name__ == '__main__':
    start_time = time.time()
    log = logging.getLogger("TASK_2_4")
    logging.basicConfig(level=logging.INFO)

    file_path = "C:/Users/DSR/PycharmProjects/truata_code_challenge/input/airbnb.parquet"
    # Initiate spark session
    log.info(" TASK_2_4 Program Started")
    try:
        spark = spark_init("Truta: TASK_2_4")

        # Loading Source data
        src_df = read_data(spark, file_path)
        dest_df = src_df.filter( (F.col("price") > 5000) & (F.col("review_scores_value") == 10) )\
            .select(F.avg("bathrooms").alias("avg_bathrooms"),F.avg("bedrooms").alias("avg_bedrooms"))
        dest_df.show()

        #Save DF to file
        save_txt(dest_df)

        log.info(" TASK_2_4 : DF transform completed")

    except Exception as e:
        log.info(" Exception: {}".format(str(e)))
        sys.exit(1)
    else:
        hours, rem = divmod(time.time() - start_time, 3600)
        mins, secs = divmod(rem, 60)
        log.info("*** TASK_2_4 Completed Successfully in : [{:0>2}hrs:{:0>2}mins:{:05.2f}secs ] ***"
                 .format(int(hours), int(mins), secs))
        sys.exit(0)
