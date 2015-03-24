using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Web;
using System.Web.DynamicData;
using System.Web.Mvc;

namespace FrontEndApp.Models
{
    public class Sensor 
    {
        public Sensor() { }

        public Sensor(Sensor sensor)
        {
            this.ID = sensor.ID;
            this.UnitID = sensor.UnitID;
            this.BuildingID = sensor.BuildingID;
        }

        [Column("ID")]
        public int ID { get; set; }

        [Column("Unit")]
        public int UnitID { get; set; }

        [Column("Building")]
        public int BuildingID { get; set; }
    }
}
