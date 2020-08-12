using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebApiCORE.Mode;
using WebAPICore.Struct;
using WebApICore.Base;
using Newtonsoft.Json.Linq;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        //[Route("Users")]
        public IActionResult GetSnapshot(string tagname)
        {
            //string[] tag_list = { "lua.fuzhuceshi" };
            string[] tag_list = tagname.Split(',');
            object lists = SnapShotManager.GetSnapShot(tag_list);
            //Value value = new Value();
            //value.user = "sa";
            //value.pass = "golden";
            var res = JToken.FromObject(lists).ToString();
            return Ok(res);
        }
    }
}
