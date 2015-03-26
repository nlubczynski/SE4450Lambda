using FrontEndApp.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;

namespace FrontEndApp.Controllers
{
    public class BuildingController : ApiController
    {
        // GET api/<controller>
        public string Get()
        {
            return JsonConvert.SerializeObject(new Powersmiths().Buildings);
        }

    }
}
