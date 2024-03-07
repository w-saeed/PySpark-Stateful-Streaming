import pandas as pd
from pyspark.sql import SparkSession, functions
#from pyspark.sql.streaming.state import GroupState
#from state_source import GroupState
from datetime import datetime, timedelta
from file_reader_info import FileReaderInfo
from type_converter import TypeConverter


# Get info from json file 
json_file = FileReaderInfo("config.json")
machine_name = json_file.get_machine_name()
sensors_list = json_file.get_sensors_list()
sensors_count = json_file.get_sensors_number()


def get_highest_sensor_values(key, dataframe, state):
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
                    #reshaped_array = [previous_state[i:i+3] for i in range(0, len(previous_state), 3)]
                    #previous_state = pd.DataFrame(reshaped_array, columns=['timestamp', 'sensor', 'value'])
                    previous_state = TypeConverter().tuple_to_df(previous_state)
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
                        'timestamp': [timestamp for _ in range(sensors_count)],
                        'sensor': sensors_list,
                        'value': [0] * sensors_count
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
                    tuple_representation = TypeConverter().df_to_tuple(init_state)
                    #tuple_representation = tuple(init_state.values.flatten())
                    #print(tuple_representation)
                    state.update(tuple_representation)
        state.setTimeoutDuration(1000)
    yield df