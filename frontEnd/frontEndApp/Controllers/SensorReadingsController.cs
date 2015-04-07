using frontEndApp.Models;
using FrontEndApp.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.UI.WebControls;

namespace FrontEndApp.Controllers
{
    public class SensorDataController : ApiController
    {
        private class Group<TKey>
        {
            public Group(TKey key, List<SensorReading> value) { this.Key = key; this.Values = value; }
            public TKey Key;
            public List<SensorReading> Values;
        }

        // GET api/<controller>
        public string Get(long start, long end, int buildingID, bool lambda)
        {
            DateTime startTime = new DateTime(1970, 1, 1).AddMilliseconds(start);
            DateTime endTime = new DateTime(1970, 1, 1).AddMilliseconds(end);

            // Create required objects            
            IEnumerable<SensorReading> enumerable;

            // If lambda, query the merge layer for the values, put them into json, and continue
            if (lambda)
            {
                WebClient wc = new WebClient();
                try
                {
                    var result =
                        wc.DownloadString("http://129.100.225.187:4880/MergeLayerServer/QueryLayerMerge?id=" + buildingID + "&start=" + start + "&end=" + end);

                    // Parse the result
                    //JavaScriptSerializer js = new JavaScriptSerializer();
                    List<JsonSensorReading> jsonSensorReadings = JsonConvert.DeserializeObject<List<JsonSensorReading>>(result);

                    // Convert to sensorReading
                    enumerable =
                        jsonSensorReadings.Select(jsr => new SensorReading(jsr.SensorId, new DateTime(1970, 1, 1).AddMilliseconds(jsr.Time), jsr.Value)).ToList();

                }
                catch
                {
                    return "Error";
                }
            }
            // if not lambda, query the mysql database for the values
            else
            {
                Powersmiths context = new Powersmiths();
                context.Database.CommandTimeout = 60 * 10;
                enumerable = context.SensorReadings;
            }            

            // check the time is valid and that the sensorID is valid
            if (endTime > startTime && start >= 0)
            {
                TimeSpan difference = endTime - startTime;

                if (difference.Days > 30)
                {
                    return MonthAggregate(startTime, endTime, buildingID, enumerable);
                }
                else if (difference.Days > 3)
                {
                    return DayAggregate(startTime, endTime, buildingID, enumerable);
                }
                else if (difference.Hours > 12)
                {
                    return HourAggregate(startTime, endTime, buildingID, enumerable);
                }
                else if (difference.Minutes > 30)
                {
                    return MinuteAggregate(startTime, endTime, buildingID, enumerable);
                }
                else
                {
                    return SecondAggregate(startTime, endTime, buildingID, enumerable);
                }
            }
            else
            {
                return "Invalid date range";
            }
        }

        private string SecondAggregate(DateTime startTime, DateTime endTime, int sensorID, IEnumerable<SensorReading> enumerable)
        {
            // Run the query
            var list = getList(startTime, endTime, sensorID, enumerable,
                sensorReading => new { sensorReading.Time.Year, sensorReading.Time.Month, sensorReading.Time.Day, 
                    sensorReading.Time.Hour, sensorReading.Time.Minute, sensorReading.Time.Second });

            // Convert to usable format, and sort
            var output = list.Select(row =>
                new double[]{ 
                    (new DateTime( row.Key.Year, row.Key.Month, row.Key.Day, row.Key.Hour, row.Key.Minute, row.Key.Second, 1, DateTimeKind.Utc) 
                        - new DateTime(1970,1,1)).TotalMilliseconds, 
                    row.Values.Average(sensorReading => sensorReading.Value)
                }
            )
            .OrderBy(row => row[0]);

            // Serialize and return
            return JsonConvert.SerializeObject(output);
        }

        private string MinuteAggregate(DateTime startTime, DateTime endTime, int sensorID, IEnumerable<SensorReading> enumerable)
        {
            // Run the query
            var list = getList(startTime, endTime, sensorID, enumerable,
                sensorReading => new { sensorReading.Time.Year, sensorReading.Time.Month, sensorReading.Time.Day, 
                    sensorReading.Time.Hour, sensorReading.Time.Minute });

            // Convert to usable format, and sort
            var output = list.Select(row =>
                new double[]{ 
                    (new DateTime( row.Key.Year, row.Key.Month, row.Key.Day, row.Key.Hour, row.Key.Minute, 1, 1, DateTimeKind.Utc) - new DateTime(1970,1,1)).TotalMilliseconds, 
                    row.Values.Average(sensorReading => sensorReading.Value)
                }
            )
            .OrderBy(row => row[0]);

            // Serialize and return
            return JsonConvert.SerializeObject(output);
        }

        private string HourAggregate(DateTime startTime, DateTime endTime, int sensorID, IEnumerable<SensorReading> enumerable)
        {
            // Run the query
            var list = getList(startTime, endTime, sensorID, enumerable,
                sensorReading => new { sensorReading.Time.Year, sensorReading.Time.Month, sensorReading.Time.Day, 
                    sensorReading.Time.Hour });

            // Convert to usable format, and sort
            var output = list.Select(row =>
                new double[]{ 
                    (new DateTime( row.Key.Year, row.Key.Month, row.Key.Day, row.Key.Hour, 1, 1, 1, DateTimeKind.Utc) - new DateTime(1970,1,1)).TotalMilliseconds, 
                    row.Values.Average(sensorReading => sensorReading.Value)
                }
            )
            .OrderBy(row => row[0]);

            // Serialize and return
            return JsonConvert.SerializeObject(output);
        }

        private string DayAggregate(DateTime startTime, DateTime endTime, int sensorID, IEnumerable<SensorReading> enumerable)
        {
            // Run the query
            var list = getList(startTime, endTime, sensorID, enumerable,
                sensorReading => new { sensorReading.Time.Year, sensorReading.Time.Month, sensorReading.Time.Day });

            // Convert to usable format, and sort
            var output = list.Select(row =>
                new double[]{ 
                    (new DateTime( row.Key.Year, row.Key.Month, row.Key.Day, 1, 1, 1, 1, DateTimeKind.Utc) - new DateTime(1970,1,1)).TotalMilliseconds, 
                    row.Values.Average(sensorReading => sensorReading.Value)
                }
            )
            .OrderBy(row => row[0]);

            // Serialize and return
            return JsonConvert.SerializeObject(output);
        }

        private string MonthAggregate(DateTime startTime, DateTime endTime, int sensorID, IEnumerable<SensorReading> enumerable)
        {
            // Run the query
            var list = getList(startTime, endTime, sensorID, enumerable, 
                sensorReading => new { sensorReading.Time.Month, sensorReading.Time.Year });

            // Convert to usable format, and sort
            var output = list.Select(row =>
                new double[]{ 
                    (new DateTime( row.Key.Year, row.Key.Month, 1, 1, 1, 1, 1, DateTimeKind.Utc) - new DateTime(1970,1,1)).TotalMilliseconds, 
                    row.Values.Average(sensorReading => sensorReading.Value)
                }
            )
            .OrderBy(row => row[0]);

            // Serialize and return
            return JsonConvert.SerializeObject(output);
        }

        private List<Group<TKey>> getList<TKey>(DateTime startTime, DateTime endTime,
            int buildingID, IEnumerable<SensorReading> enumerable, Func<SensorReading, TKey> groupBy)
        {
            Powersmiths context = new Powersmiths();
            return enumerable
                .Join(
                    context.Sensors, 
                    sensorReading => sensorReading.SensorId,
                    sensor => sensor.ID,
                    (sensorReading, sensor) => new {SensorReading = sensorReading, Sensor = sensor})
                .Join(
                    context.Buildings,
                    sensorReading => sensorReading.Sensor.BuildingID,
                    building => building.ID,
                    (sensorReading, building) => new { SensorReading = sensorReading.SensorReading, Sensor = sensorReading.Sensor, Building = building })
               .Where(sensorReading =>
                   sensorReading.SensorReading.Time >= startTime &&
                   sensorReading.SensorReading.Time <= endTime &&
                   sensorReading.Building.ID == buildingID)
               .Select(result => new SensorReading(result.SensorReading))
               .GroupBy(groupBy)
               .Select(group => new Group<TKey>(group.Key, group.ToList()))
               .ToList();
        }

        // POST api/<controller>
        public void Post([FromBody]string value)
        {
        }

        // PUT api/<controller>/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        public void Delete(int id)
        {
        }
    }
}