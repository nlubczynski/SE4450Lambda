using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;

namespace FrontEndApp.Models
{
    public class Powersmiths : DbContext
    {
        public DbSet<Sensor> Sensors { get; set; }
        public DbSet<Building> Buildings { get; set; }
        public DbSet<SensorReading> SensorReadings { get; set; }
        public DbSet<Unit> Units { get; set; }
    }
}