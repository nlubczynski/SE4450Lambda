using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace frontEndApp.Models
{
    public class JsonSensorReading
    {
        [JsonProperty("timestamp")]
        public Int64 Time { get; set; }
        [JsonProperty("sensorID")]
        public int SensorId { get; set; }
        [JsonProperty("value")]
        public int Value { get; set; }
    }
}