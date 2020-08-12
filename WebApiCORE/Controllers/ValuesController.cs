using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebApiCORE.Mode;
using WebAPICore.Struct;
using WebApICore.Base;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Runtime.CompilerServices;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        //[Route("Users")]
        [HttpGet]
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

        [HttpPost("{id}")]
        public IActionResult PutSnapshot([FromBody] object obj)
        {
            //var str = "hello";
            var DynamicObject = JsonConvert.SerializeObject(obj);
            //string[] tag_list = tagname.Split(',');
            //object lists = SnapShotManager.GetSnapShot(tag_list);
            return Ok(DynamicObject);
        }
    }
}
