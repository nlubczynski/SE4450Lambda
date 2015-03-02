# Speed Layer 
 
An implementation of Storm to handle real-time data processing for the Lambda Architecture.

## System Design

1. Messages are sent to the system through Kafka, under the "sensorData" topic, in the format "sensorID sensorValue unix timestamp".
2. The storm topology handles the incoming information
  * Writing the raw values to the HDFS
  * Processing the data, calculating averages, aggregating data, and other data processing tasks.
  * Write the processed data to the HBase table "SensorValuesSpeedLayer"

## Project

As a fourth year design project, we are implementing a prototype Lambda Architecture system to gauge real-world feasibility of such a platform for a corporate partner. The system will be gauged against traditional implementations, such as MySQL, to verify whether Lambda Architecture is a worthwhile investment.

The system is being designed to handle streaming data in the form of sensor readings.  The system is also expected to serve this data in real time. As the number of sensors, and the frequency of their readings, increase, we imagine traditional SQL system may become overwhelmed. Our goal is to determine at which point this occurs, if all, to determine whether Lambda Architecture is a viable option.


## Authors
Brandon McRae
Nik Lubczynski
Braden Lunn
