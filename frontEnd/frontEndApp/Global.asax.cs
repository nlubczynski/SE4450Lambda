using frontEndApp.Hubs;
using FrontEndApp.Models;
using Microsoft.AspNet.SignalR;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Timers;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;

namespace FrontEndApp
{
    public class MvcApplication : System.Web.HttpApplication
    {
        // Main repository
        private Powersmiths data;

        // Data sets
        private List<Sensor> _sensors;
        private List<SensorReading> _sensorReadingsSeconds;
        private List<SensorReading> _sensorReadingsMinutes;
        private List<SensorReading> _sensorReadingsHours;
        private List<SensorReading> _sensorReadingsDays;
        private List<SensorReading> _sensorReadingsMonths;
        private List<Building> _buidlings;
        private List<Unit> _units;        

        // Instance
        public static MvcApplication Instance { get; private set; }

        // Mutexes for retrieving data
        private static Mutex buildingMutex = new Mutex();
        private static Mutex sensorMutex = new Mutex();
        private static Mutex sensorReadingSecMutex = new Mutex();
        private static Mutex sensorReadingMinMutex = new Mutex();
        private static Mutex sensorReadingHourMutex = new Mutex();
        private static Mutex sensorReadingDayMutex = new Mutex();
        private static Mutex sensorReadingMonthMutex = new Mutex();
        private static Mutex unitMutex = new Mutex();

        // Main application entry point
        protected void Application_Start()
        {
            if (Instance == null)
                Instance = this;

            AreaRegistration.RegisterAllAreas();

            WebApiConfig.Register(GlobalConfiguration.Configuration);
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            // Initiliaze data
            data = new Powersmiths();
            Sensors = new List<Sensor>();
            SensorReadingsSeconds = new List<SensorReading>();
            SensorReadingsMinutes = new List<SensorReading>();
            SensorReadingsHours = new List<SensorReading>();
            SensorReadingsDays = new List<SensorReading>();
            SensorReadingsMonths = new List<SensorReading>();
            Units = new List<Unit>();
            Buildings = new List<Building>();
            update();

            // Update loop
            Thread updateThread = new Thread(updateLoop);
            updateThread.Start();
        }

        private void updateLoop()
        {
            for (; ; )
            {
                update();
            }
        }

        private void update()
        {
            // Initialize swap variables
            List<Sensor> swapSensors = new List<Sensor>();
            List<SensorReading> swapSensorReadingsSecond = new List<SensorReading>();
            List<SensorReading> swapSensorReadingsMin = new List<SensorReading>();
            List<SensorReading> swapSensorReadingsHour = new List<SensorReading>();
            List<SensorReading> swapSensorReadingsDay = new List<SensorReading>();
            List<SensorReading> swapSensorReadingsMonth = new List<SensorReading>();
            List<Building> swapBuildings = new List<Building>();
            List<Unit> swapUnits = new List<Unit>();

            // Sensors
            swapSensors = data.Sensors.ToList();

            // Sensor Readings
            parseSensorTime(
                data.SensorReadings.OrderBy(sensorReading => sensorReading.Time).ToList(),
                swapSensorReadingsSecond,
                swapSensorReadingsMin,
                swapSensorReadingsHour,
                swapSensorReadingsDay,
                swapSensorReadingsMonth);

            // Buildings
            swapBuildings = data.Buildings.ToList();

            // Units
            swapUnits = data.Units.ToList();

            // Swap Buildings
            buildingMutex.WaitOne();
            Buildings = swapBuildings;
            buildingMutex.ReleaseMutex();

            // Swap sensors
            sensorMutex.WaitOne();
            Sensors = swapSensors;
            sensorMutex.ReleaseMutex();

            // Swap sensorReadings minutes
            sensorReadingSecMutex.WaitOne();
            SensorReadingsSeconds = swapSensorReadingsSecond;
            sensorReadingSecMutex.ReleaseMutex();

            // Swap sensorReadings minutes
            sensorReadingMinMutex.WaitOne();
            SensorReadingsMinutes = swapSensorReadingsMin;
            sensorReadingMinMutex.ReleaseMutex();

            // Swap sensorReadings minutes
            sensorReadingHourMutex.WaitOne();
            SensorReadingsHours = swapSensorReadingsHour;
            sensorReadingHourMutex.ReleaseMutex();

            // Swap sensorReadings days
            sensorReadingDayMutex.WaitOne();
            SensorReadingsDays = swapSensorReadingsDay;
            sensorReadingDayMutex.ReleaseMutex();

            // Swap sensorReadings months
            sensorReadingMonthMutex.WaitOne();
            SensorReadingsMonths = swapSensorReadingsMonth;
            sensorReadingMonthMutex.ReleaseMutex();

            // Swap units
            unitMutex.WaitOne();
            Units = swapUnits;
            unitMutex.ReleaseMutex();

            // Trigger client update
            GlobalHost.ConnectionManager.GetHubContext<UpdateHub>().Clients.All.updateMySQL();
        }

        private void parseSensorTime(List<SensorReading> input, List<SensorReading> secOut, List<SensorReading> minOut, List<SensorReading> hourOut, List<SensorReading> dayOut, List<SensorReading> monthOut)
        {
            // Group sensorReadings by year
            var groupedByYear = input.GroupBy(sensorReading => sensorReading.Time.Year);

            foreach(var yearGroup in groupedByYear)
            {
                // Group those sensorReadings by month
                var groupedByMonth = yearGroup.GroupBy(sensorReading => sensorReading.Time.Month);

                foreach(var monthGroup in groupedByMonth)
                {
                    // Group by id, and average out for the month
                    var monthGroupedByID = monthGroup.GroupBy(sensorReading => sensorReading.SensorId);
                    foreach(var idMonthGroup in monthGroupedByID)
                    {
                        monthOut.Add(averageSensorReadings(idMonthGroup.ToList()));
                    }

                    // Group by day
                    var groupedByDay = monthGroup.GroupBy(sensorReading => sensorReading.Time.Day);
                    foreach(var dayGroup in groupedByDay)
                    {                        
                        //Group by id, and average out for the day
                        var dayGroupedById = dayGroup.GroupBy(sensorReading => sensorReading.SensorId);
                        foreach (var idDayGroup in dayGroupedById)
                        {
                            dayOut.Add(averageSensorReadings(idDayGroup.ToList()));
                        }

                        // Group by hour
                        var groupedByHour = dayGroup.GroupBy(sensorReading => sensorReading.Time.Hour);
                        foreach(var hourGroup in groupedByHour)
                        {
                            //Group by id, and average out for the hour
                            var hourGroupedByID = hourGroup.GroupBy(sensorReading => sensorReading.SensorId);
                            foreach (var idHourGroup in hourGroupedByID)
                            {
                                hourOut.Add(averageSensorReadings(idHourGroup.ToList()));
                            }

                            // Group by minute
                            var groupedByMinute = hourGroup.GroupBy(sensorReading => sensorReading.Time.Minute);
                            foreach(var minuteGroup in groupedByMinute)
                            {
                                //Group by id, and average out for the minute
                                var minuteGroupedById = minuteGroup.GroupBy(sensorReading => sensorReading.SensorId);
                                foreach (var idMinuteGroup in minuteGroupedById)
                                {
                                    minOut.Add(averageSensorReadings(idMinuteGroup.ToList()));
                                }

                                // Group by second
                                var groupedBySecond = minuteGroup.GroupBy(sensorReading => sensorReading.Time.Second);
                                foreach(var secondGroup in groupedBySecond)
                                {
                                    //Group by id, and average out for the second
                                    var secondGroupById = secondGroup.GroupBy(sensorReading => sensorReading.SensorId);
                                    foreach (var idSecondGroup in secondGroupById)
                                    {
                                        secOut.Add(averageSensorReadings(idSecondGroup.ToList()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private SensorReading averageSensorReadings(List<SensorReading> input)
        {
            SensorReading returnVal = new SensorReading();

            foreach(SensorReading reading in input)
            {
                returnVal.Value += reading.Value;
            }

            if (input.Count > 0)
            {
                returnVal.SensorId = input[0].SensorId;
                returnVal.Time = input[0].Time;
                returnVal.Value = returnVal.Value / input.Count;
            }

            return returnVal;
        }

        public List<Sensor> Sensors
        {
            get
            {
                sensorMutex.WaitOne();
                // copy to new list
                List<Sensor> returnVal = _sensors.ConvertAll(sensor => new Sensor(sensor));
                sensorMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensors = value; }
        }
        public List<Unit> Units
        {
            get
            {
                unitMutex.WaitOne();
                // copy to new list
                List<Unit> returnVal = _units.Select(unit => unit).ToList();
                unitMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _units = value; }
        }
        public List<Building> Buildings
        {
            get
            {
                buildingMutex.WaitOne();
                // copy to new list
                List<Building> returnVal = _buidlings.ConvertAll(building => new Building(building));
                buildingMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _buidlings = value; }
        }
        public List<SensorReading> SensorReadingsSeconds
        {
            get
            {
                sensorReadingSecMutex.WaitOne();
                // copy to new list
                List<SensorReading> returnVal = _sensorReadingsSeconds.ConvertAll(sensorReading => new SensorReading(sensorReading));
                sensorReadingSecMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensorReadingsSeconds = value; }
        }
        public List<SensorReading> SensorReadingsMinutes
        {
            get
            {
                sensorReadingMinMutex.WaitOne();
                // copy to new list
                List<SensorReading> returnVal = _sensorReadingsMinutes.ConvertAll(sensorReading => new SensorReading(sensorReading));
                sensorReadingMinMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensorReadingsMinutes = value; }
        }
        public List<SensorReading> SensorReadingsHours
        {
            get
            {
                sensorReadingHourMutex.WaitOne();
                // copy to new list
                List<SensorReading> returnVal = _sensorReadingsHours.ConvertAll(sensorReading => new SensorReading(sensorReading));
                sensorReadingHourMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensorReadingsHours = value; }
        }
        public List<SensorReading> SensorReadingsDays
        {
            get
            {
                sensorReadingDayMutex.WaitOne();
                // copy to new list
                List<SensorReading> returnVal = _sensorReadingsDays.ConvertAll(sensorReading => new SensorReading(sensorReading));
                sensorReadingDayMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensorReadingsDays = value; }
        }
        public List<SensorReading> SensorReadingsMonths
        {
            get
            {
                sensorReadingMonthMutex.WaitOne();
                // copy to new list
                List<SensorReading> returnVal = _sensorReadingsMonths.ConvertAll(sensorReading => new SensorReading(sensorReading));
                sensorReadingMonthMutex.ReleaseMutex();
                return returnVal;
            }
            private set { _sensorReadingsMonths = value; }
        }
    }
}