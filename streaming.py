from pyspark.sql import SparkSession
import random
import time
from typing import Iterator, Any
import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark.sql.streaming.state import GroupState
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from file_reader_info import FileReaderInfo
from state_func import get_highest_sensor_values


# Create a SparkSession
spark = SparkSession.builder \
    .appName("StreamingDataGenerator") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# Get info from json file 
json_file = FileReaderInfo("machine.json")
machine_name = json_file.get_machine_name()
sensors_list = json_file.get_sensors_list()
sensors_count = json_file.get_sensors_number()

# State output structure
initial_list = [
    StructField("timestamp", TimestampType()), 
    StructField("sensor", StringType()),
    StructField("value", IntegerType()),
]
state_structure = initial_list * sensors_count


def select_name(index):
    return sensors_list[index]
# Register the UDF
spark.udf.register("select_name_udf", select_name, StringType())

# Create a streaming DataFrame
stream_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .selectExpr("value as id") \
    .withColumn("names_index", col("id") % len(sensors_list)) \
    .withColumn("machine_name", lit(machine_name)) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("sensor", expr("select_name_udf(names_index)")) \
    .withColumn("value", (rand() * 1000).cast("integer")) \
    .drop("names_index") \
    .drop("id") \


# use the stateful function
stream= stream_df \
    .groupBy("machine_name") \
    .applyInPandasWithState(
    func=get_highest_sensor_values,
    outputStructType=StructType([
        StructField("machine_name", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("sensor", StringType()),
        StructField("value", IntegerType())
    ]),
    stateStructType=StructType(state_structure),
    outputMode="update",
    timeoutConf="ProcessingTimeTimeout"
)

write_query = stream.writeStream.outputMode("update").format("console").option("checkpointLocation", "checkpoint").option("truncate", False).start()
write_query.awaitTermination()
