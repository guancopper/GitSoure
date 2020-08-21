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
using WebApiCORE.Base;

namespace WebApiCORE.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ExcelLUAController : ControllerBase
    {
        public IActionResult GetExcelStation(string StartTime,string EndTime,int Interval, int count,string tagname)
        {
            try
            {
                string tagnamestr = tagname.ToString();
                string[] tag_list = tagnamestr.Split(',');
                Dictionary<string, List<DataEntity>> dicE = new Dictionary<string, List<DataEntity>>();
                Dictionary<string, List<Summary>> dicS = new Dictionary<string, List<Summary>>();
                if (Interval == 1)
                {
                    dicE = History.Get(tag_list,StartTime,EndTime);
                }
                else
                {
                    dicS = HistorySummary.GetNumberSummary(tag_list, StartTime, EndTime, Interval, count);
                }
                List<ExcelLUA> listexcel = new List<ExcelLUA>();
                for (int i = 0; i < count; i++)
                {
                    ExcelLUA info = new ExcelLUA();
                    DateTime dateTime = Convert.ToDateTime(StartTime);
                    info.DateTime = dateTime.AddSeconds(i * Interval).ToString("yyyy-MM-dd HH:mm:ss");
                    for (int j = 0; j < tag_list.Length; j++)
                    {
                        bool flag = false;
                        if (Interval == 1)
                        {
                            flag = dicE.TryGetValue(tag_list[j], out List<DataEntity> list);
                            if (flag)
                            {
                                info.Total = list.Count.ToString();
                                switch (tag_list[j])
                                {
                                    case "LUA.Totalactivepower":
                                        info.Totalactivepower = list[i].Value.ToString();
                                        break;
                                    case "LUA.p_set_adjust_help":
                                        info.p_set_adjust_help = list[i].Value.ToString();
                                        break;
                                    case "LUA.IsParkControlIntoService":
                                        info.IsParkControlIntoService = list[i].Value.ToString();
                                        break;
                                    case "LUA.IsAGCRemote":
                                        info.IsAGCRemote = list[i].Value.ToString();
                                        break;
                                    case "LUA.avr_windspeed":
                                        info.avr_windspeed = list[i].Value.ToString();
                                        break;
                                    case "LUA.NumWT_in_Power_production":
                                        info.NumWT_in_Power_production = list[i].Value.ToString();
                                        break;
                                    case "LUA.NumWT_comfail":
                                        info.NumWT_comfail = list[i].Value.ToString();
                                        break;
                                    case "LUA.Q_total":
                                        info.Q_total = list[i].Value.ToString();
                                        break;
                                    case "LUA.Q_set_point":
                                        info.Q_set_point = list[i].Value.ToString();
                                        break;
                                    case "LUA.Is_Q_ParkControlIntoService":
                                        info.Is_Q_ParkControlIntoService = list[i].Value.ToString();
                                        break;
                                    case "LUA.IsAVCRemote":
                                        info.IsAVCRemote = list[i].Value.ToString();
                                        break;
                                };
                            }
                        }
                        else
                        {
                            flag = dicS.TryGetValue(tag_list[j], out List<Summary> list);
                            if (flag)
                            {
                                info.Total = list.Count.ToString();
                                switch (tag_list[j])
                                {
                                    case "LUA.Totalactivepower":
                                        info.Totalactivepower = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.p_set_adjust_help":
                                        info.p_set_adjust_help = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.IsParkControlIntoService":
                                        info.IsParkControlIntoService = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.IsAGCRemote":
                                        info.IsAGCRemote = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.avr_windspeed":
                                        info.avr_windspeed = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.NumWT_in_Power_production":
                                        info.NumWT_in_Power_production = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.NumWT_comfail":
                                        info.NumWT_comfail = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.Q_total":
                                        info.Q_total = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.Q_set_point":
                                        info.Q_set_point = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.Is_Q_ParkControlIntoService":
                                        info.Is_Q_ParkControlIntoService = list[i].CalcAvg.ToString();
                                        break;
                                    case "LUA.IsAVCRemote":
                                        info.IsAVCRemote = list[i].CalcAvg.ToString();
                                        break;
                                };
                            }
                        }
                    }
                    listexcel.Add(info);
                }
                var res = JToken.FromObject(listexcel).ToString();
                return Ok(res);
            }
            catch{
                return Ok();
            }
        }
    }
}
