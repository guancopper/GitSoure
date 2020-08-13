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
using WebAPICore.Method;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        //[Route("Users")]
        //[HttpGet("{tagname}")]
        public IActionResult GetSnapshot(string tagname)
        {
            //string[] tag_list = { "lua.fuzhuceshi" };
            string tagnamestr = tagname.ToString();
            string[] tag_list = tagnamestr.Split(',');
            object lists = SnapShotManager.GetSnapShot(tag_list);
            //Value value = new Value();
            //value.user = "sa";
            //value.pass = "golden";
            var res = JToken.FromObject(lists).ToString();
            return Ok(res);
        }

        //[HttpPost("{id}")]
        public IActionResult PutSnapshot(object obj)
        {
            var jsonstr = JsonConvert.SerializeObject(obj);
            var DynamicObject = JsonConvert.DeserializeObject<dynamic>(jsonstr);
            //string str = DynamicObject.TagName;
            List<PointData> points = new List<PointData>();
            foreach (var point in DynamicObject)
            {
                points.Add(new PointData()
                {
                    TagName = point.TagName,
                    Time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    Value = point.Value,
                    Quality = point.Quality,
                });
            }
            var er = Snapshot.PutSnapshot(points);
            return Ok(er);
        }
    }
}
