using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebAPICore.Method;
using WebApiCORE.Mode;
using WebAPICore.Struct;
using WebApICore.Base;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Runtime.CompilerServices;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class InfoController : ControllerBase
    {
        public IActionResult GetInfo(int start,int end, string tagname)
        {
            try {
                string tagnamestr = tagname.ToString();
                string[] tag_list = tagnamestr.Split(',');
                List<PointData> lists = SnapShotManager.GetSnapShot(tag_list);
                List<Info> listinfo = new List<Info>();
                for (int j = 0, i = 0; i <= end-start; i++, j = j + 5)
                {
                    PointData point = lists[i];
                    Info info = new Info();
                    info.WTID = (start+i).ToString();
                    info.activepowerkw = lists[j].Value;
                    info.reactivepowerkv = lists[j + 1].Value;
                    info.activepowerdemand = lists[j + 2].Value;
                    info.reactivepowerdemand = lists[j + 3].Value;
                    info.Windspeed = lists[j + 4].Value;
                    listinfo.Add(info);
                }
                var res = JToken.FromObject(listinfo).ToString();
                return Ok(res);
            } catch {
                return Ok();
            }
            
        }
    }
}
