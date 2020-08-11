using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebApiCORE.Mode;
using WebAPICore.Struct;
using WebApICore.Base;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        //[Route("Users")]
        public IActionResult Get()
        {
            string[] tag_list = { "lua.fuzhuceshi" };
            List<PointData> lists = SnapShotManager.GetSnapShot(tag_list);
            //Value value = new Value();
            //value.user = "sa";
            //value.pass = "golden";
            return Ok(lists);
        }
    }
}
