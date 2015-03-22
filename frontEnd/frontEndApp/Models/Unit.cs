using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FrontEndApp.Models
{
    public class Unit
    {
        public Unit() { }

        public Unit(Unit unit)
        {
            this.Name = unit.Name;
            this.ID = unit.ID;
        }

        public String Name { get; set; }
        public int ID { get; set; }
    }
}
