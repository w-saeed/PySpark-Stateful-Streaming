import json
import os
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



class TypeConverter:
    def df_to_tuple(self, df):
        tuple_state = tuple(df.values.flatten())
        return tuple_state

    def tuple_to_df(self, tuple):
        reshaped_array = [tuple[i:i+3] for i in range(0, len(tuple), 3)]
        df_state = pd.DataFrame(reshaped_array, columns=['timestamp', 'sensor', 'value'])
        return df_state
