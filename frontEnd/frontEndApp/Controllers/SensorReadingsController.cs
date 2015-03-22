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
        // GET api/<controller>
        public string Get(long start, long end)
        {

            DateTime startTime = new DateTime(1970, 1, 1).AddMilliseconds(start);
            DateTime endTime = new DateTime(1970, 1, 1).AddMilliseconds(end);

            List<SensorReading> list = MvcApplication.Instance.SensorReadingsMonths;

            if(endTime > startTime)
            {
                TimeSpan difference = endTime - startTime;

                if (difference.Days > 30)
                {
                    list = MvcApplication.Instance.SensorReadingsMonths;
                }
                else if (difference.Days > 3)
                {
                    list = MvcApplication.Instance.SensorReadingsDays;
                }
                else if (difference.Hours > 12)
                {
                    list = MvcApplication.Instance.SensorReadingsHours;
                }
                else if (difference.Minutes > 30)
                {
                    list = MvcApplication.Instance.SensorReadingsMinutes;
                }
                else
                {
                    list = MvcApplication.Instance.SensorReadingsSeconds;
                }
            }
            else
            {
                return "Invalid date range";
            }

            // get only within the range
            list = list.Where(sensorReading => sensorReading.Time >= startTime && sensorReading.Time <= endTime).ToList();

            // Group by id
            var groupedByID = list.GroupBy(sensorReading => sensorReading.SensorId);

            // Bring out into list of lists, and convert to unix time
            List<List<double[]>> lists = new List<List<double[]>>();
            foreach(var group in groupedByID)
            {
                lists.Add(new List<double[]>(group.Select(reading => new double[] { 
                    (reading.Time - new DateTime(1970, 1, 1)).TotalMilliseconds, reading.Value 
                })));
            }

            // Serialize and return
            return JsonConvert.SerializeObject(lists);
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