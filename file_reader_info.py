import json
import os

class FileReaderInfo:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = self._load_data()

    def _load_data(self):
        with open(self.file_path, "r") as json_file:
            return json.load(json_file)

    def get_machine_name(self):
        # Get the value of the "machine" key
        machine_value = self.data["machine"]
        return machine_value

    def get_sensors_list(self):
        # Extract all sensor names
        sensor_names = [sensor["sensor_name"] for sensor in self.data["sensors"]]
        return sensor_names

    def get_sensors_number(self):
        # Get the count of sensor items
        sensor_count = len(self.data["sensors"])
        return sensor_count
