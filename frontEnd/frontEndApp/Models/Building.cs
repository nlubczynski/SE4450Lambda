using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FrontEndApp.Models
{
    public class Building 
    {
        public Building() { }

        public Building(Building building)
        {
            this.ID = building.ID;
            this.Name = building.Name;
        }

        [Column("id")]
        public int ID { get; set; }
        [Column("name")]
        public string Name { get; set; }
    }
}
