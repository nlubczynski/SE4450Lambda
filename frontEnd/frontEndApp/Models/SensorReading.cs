using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FrontEndApp.Models
{
    public class SensorReading
    {
        public SensorReading() { }

        public SensorReading(SensorReading sensorReading)
        {
            this.ID = sensorReading.ID;
            this.SensorId = sensorReading.SensorId;
            this.Time = sensorReading.Time;
            this.Value = sensorReading.Value;
        }

        public SensorReading(int sensorId, DateTime time, int value)
        {
            this.ID = 0;
            this.SensorId = sensorId;
            this.Time = time;
            this.Value = value;
        }

        public int ID { get; set; }
        public int SensorId { get; set; }
        public DateTime Time { get; set; }
        public int Value { get; set; }
    }
}
