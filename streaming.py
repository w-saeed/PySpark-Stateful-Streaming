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


# Create a SparkSession
spark = SparkSession.builder \
    .appName("StreamingDataGenerator") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

names_list = ["sensor1", "sensor2", "sensor3", "sensor4"]
# Define a UDF to select a name from the list based on the index
def select_name(index):
    return names_list[index]

# Register the UDF
spark.udf.register("select_name_udf", select_name, StringType())

# Create a streaming DataFrame
stream_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .selectExpr("value as id") \
    .withColumn("names_index", col("id") % len(names_list)) \
    .withColumn("machine_name", lit("XYZ")) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("sensor", expr("select_name_udf(names_index)")) \
    .withColumn("value", (rand() * 1000).cast("integer")) \
    .drop("names_index") \
    .drop("id") \


def highest_value_for_sensors(key, dataframe, state):
    session_state = "active"
    if state.hasTimedOut:
        session_state = "timed_out"
        state.remove()
    else:
        for df in dataframe:
                if state.exists:
                    # Get the existing state
                    previous_state = state.get
                    #print(previous_state)
                    reshaped_array = [previous_state[i:i+3] for i in range(0, len(previous_state), 3)]
                    previous_state = pd.DataFrame(reshaped_array, columns=['timestamp', 'sensor', 'value'])
                    print("-------------------------------------------")
                    print("State: ")
                    print("-------------------------------------------")
                    print(previous_state)

                    # Iterate over rows in df_batch
                    for index, row in df.iterrows():
                        sensor = row['sensor']
                        value = row['value']
                        timestamp = row['timestamp']
                        # Check if value in df_batch is greater than corresponding value in df_init
                        if value > previous_state.loc[previous_state['sensor'] == sensor, 'value'].iloc[0]:
                            # Update df_init value and timestamp
                            previous_state.loc[previous_state['sensor'] == sensor, ['value', 'timestamp']] = [value, timestamp]

                    # Converte the Dataframe in to a tuple and update the State
                    tuple_representation = tuple(previous_state.values.flatten())
                    df_init =previous_state
                    #print(tuple_representation)
                    state.update(tuple_representation)
                    
                else:
                    # get all unique senosr with the largest value from the Batch 
                    idx = df.groupby('sensor')['value'].idxmax()
                    df_batch = df.loc[idx]
                    # init df
                    timestamp = df.at[0, 'timestamp']
                    init_state = pd.DataFrame({
                        'timestamp': [timestamp for _ in range(4)],
                        'sensor': ['sensor1', 'sensor2', 'sensor3', 'sensor4'],
                        'value': [0, 0, 0, 0]
                    })
                    print("-------------------------------------------")
                    print("State: ")
                    print("-------------------------------------------")
                    print(init_state)
                    # Function to update df_init based on condition
                    def update_df_init(row):
                        sensor = row['sensor']
                        value = row['value']
                        # Check condition
                        if value > init_state.loc[init_state['sensor'] == sensor, 'value'].values[0]:
                            init_state.loc[init_state['sensor'] == sensor, 'value'] = value
                    
                    # Apply the function to each row of df_batch
                    df_batch.apply(update_df_init, axis=1)
                    # Converte the Dataframe in to a tuple and update the State
                    tuple_representation = tuple(init_state.values.flatten())
                    #print(tuple_representation)
                    state.update(tuple_representation)
        state.setTimeoutDuration(1000)
    yield df


stream= stream_df \
    .groupBy("machine_name") \
    .applyInPandasWithState(
    func=highest_value_for_sensors,
    outputStructType=StructType([
        StructField("machine_name", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("sensor", StringType()),
        StructField("value", IntegerType())
    ]),
    stateStructType=StructType([StructField("timestamp", TimestampType()), StructField("sensor", StringType()), StructField("value", IntegerType()), StructField("timestamp", TimestampType()), StructField("sensor", StringType()), StructField("value", IntegerType()), StructField("timestamp", TimestampType()), StructField("sensor", StringType()), StructField("value", IntegerType()), StructField("timestamp", TimestampType()), StructField("sensor", StringType()), StructField("value", IntegerType()),]),
    outputMode="update",
    timeoutConf="ProcessingTimeTimeout"
)

write_query = stream.writeStream.outputMode("update").format("console").option("truncate", False).start()
write_query.awaitTermination()
