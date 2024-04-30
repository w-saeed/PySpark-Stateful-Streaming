# **Stateful Processing of Realtime Data**
<br />This project is designed to retrieve the highest values recorded by each sensor.
You can generate random sensor data and  customize the sensors by adding or deleting them in the configuration file.

```
{
  "machine": "xyz",
  "sensors": [{
      "sensor_name": "sensor1"
    },
    {
      "sensor_name": "sensor2"
    },
    {
      "sensor_name": "sensor3"
    },
    {
      "sensor_name": "sensor4"
    },
    {
      "sensor_name": "sensor5"
    }
  ]
}
```
## Performance Graph:
<br />
After two hours of running the script, I've created a diagram to analyse how my script is performing. This visual representation breaks down the time taken for processing each batch, giving me a clear picture of its efficiency.

![output](https://github.com/w-saeed/Pyspark-Stateful-Streaming/assets/28221354/7fa2b578-5898-4604-939e-17606a0bd766)

