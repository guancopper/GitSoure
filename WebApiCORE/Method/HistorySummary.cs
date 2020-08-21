using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GoldenAPI;
using GoldenAPI.Common;
using GoldenAPI.Impl;
using GoldenAPI.Model.Base;
using GoldenAPI.Model.Data;
using GoldenAPI.Model.Historian;
using WebApICore.Base;
using WebAPICore.Struct;

namespace WebAPICore.Method
{
    public static class HistorySummary
    {

        public static Summary GetNumberSummary(string[] tagNames, string beginTime, string endTime)
        {
            var result = new Summary();
            if (tagNames == null || tagNames.Length <= 0)
            {
                return null;
            }
            DateTime begin = Convert.ToDateTime(beginTime);
            DateTime end = Convert.ToDateTime(endTime);
            if (end > ConnectionPool.ServerTime) end = ConnectionPool.ServerTime;
            Dictionary<string, PointInfo> points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;
            SummaryEntity entity;
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                entity = his.GetNumberSummary(points[tagNames[0]].ID, begin, end);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
            if (entity == null)
            {
                return null;
            }
            result = new Summary()
            {
                Min = entity.Min,
                Max = entity.Max,
                Total = entity.Total,
                PowerAvg = entity.PowerAvg,
                StartTime = entity.StartTime.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                EndTime = entity.EndTime.ToString("yyyy-MM-dd HH:mm:ss.fff")
            };

            return result;
        }

        public static Dictionary<string, List<Summary>> GetNumberSummary(string[] tagNames, string beginTime, string endTime, int interval, int count)
        {
            Dictionary<string, List<Summary>> dicresult = new Dictionary<string, List<Summary>>();
            if (tagNames == null || tagNames.Length <= 0)
            {
                return null;
            }
            DateTime begin = Convert.ToDateTime(beginTime);
            DateTime end = Convert.ToDateTime(endTime);
            if (end > ConnectionPool.ServerTime) end = ConnectionPool.ServerTime;
            Dictionary<string, PointInfo> points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                for (int i = 0; i < points.Count; i++)
                {
                    KeyValuePair<string, PointInfo> point = points.ElementAt(i);
                    Entity<SummaryEntity> ese = his.GetNumberSummaryInBatches(point.Value.ID, count, interval * 1000, begin, end);
                    List<SummaryEntity> listentity = ese.Data;
                    if (listentity.Count == 0)
                    {
                        continue;
                    }
                    List<Summary> listsummery = new List<Summary>();
                    for (int j = 0; j < listentity.Count; j++)
                    {
                        SummaryEntity entity = listentity[j];
                        var result = new Summary()
                        {
                            Min = entity.Min,
                            Max = entity.Max,
                            Total = entity.Total,
                            CalcAvg = entity.CalcAvg,
                            PowerAvg = entity.PowerAvg,
                            StartTime = entity.StartTime.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                            EndTime = entity.EndTime.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        listsummery.Add(result);
                    }
                    dicresult.Add(point.Key, listsummery);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
            return dicresult;
        }

        public static ConcurrentDictionary<string, Summary> GetAccumulatedData(
                   string[] tagNames, string beginTime, string endTime, string dataType,
                   int thread_page_size = 5000)
        {
            ConcurrentDictionary<string, Summary> dicresult = new ConcurrentDictionary<string, Summary>();

            if (tagNames == null || tagNames.Length <= 0)
            {
                return dicresult;
            }
            DateTime begin = Convert.ToDateTime(beginTime);
            DateTime end = Convert.ToDateTime(endTime);
            DateTime lybegin = Convert.ToDateTime(beginTime).AddYears(-1);
            DateTime lyend = Convert.ToDateTime(endTime).AddYears(-1);
            if (end > ConnectionPool.ServerTime) end = ConnectionPool.ServerTime;
            Dictionary<string, PointInfo> points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0)
            {
                return dicresult;
            }
            List<Dictionary<string, PointInfo>> list =
                new List<Dictionary<string, PointInfo>>();
            Dictionary<string, PointInfo> temp = new Dictionary<string, PointInfo>();
            foreach (var kv in points)
            {
                temp.Add(kv.Key, kv.Value);
                if (temp.Count > thread_page_size)
                {
                    list.Add(temp);
                    temp = new Dictionary<string, PointInfo>();
                }
            }
            if (temp.Count != 0)
            {
                list.Add(temp);
                temp = null;
            }
            ParallelLoopResult LoopResult =
                Parallel.ForEach(list, item =>
                {
                    GetAccumulatedDataMethod(beginTime, endTime, dicresult, begin, end, lybegin, lyend, item);
                });

            return dicresult;
        }

        private static void GetAccumulatedDataMethod(string beginTime, string endTime, ConcurrentDictionary<string, Summary> dicresult, DateTime begin, DateTime end, DateTime lybegin, DateTime lyend, Dictionary<string, PointInfo> points)
        {
            var conn = ConnectionPool.TryGetConnection(0, 0);
            try
            {
                var his = new HistorianImpl(conn);
                List<int> ids = new List<int>();
                foreach (var point in points)
                {
                    ids.Add(point.Value.ID);
                }
                //var temp = (from p in points.Values select p.ID).ToArray();
                var create_dates = new BaseImpl(conn).GetCreateDates(ids.ToArray());
                for (int i = 0; i < points.Count; i++)
                {
                    try
                    {
                        double lydifference = 0;
                        double beginValue = 0;
                        KeyValuePair<string, PointInfo> point = points.ElementAt(i);
                        SummaryEntity ese = new SummaryEntity();
                        Summary summary = new Summary();
                        DataType dType = point.Value.Type;
                        switch (dType)
                        {
                            case DataType.INT16:
                            case DataType.INT32:
                            case DataType.INT64:
                            case DataType.INT8:
                            case DataType.UINT16:
                            case DataType.UINT32:
                            case DataType.UINT8:
                            case DataType.BOOL:
                                //今年
                                IntData beginValueInt = his.GetIntSingleValue(point.Value.ID, begin, HistorianMode.EXACT);
                                if (beginValueInt == null)
                                {
                                    //没有准确值，则取上一个值
                                    beginValueInt = his.GetIntSingleValue(point.Value.ID, begin, HistorianMode.PREVIOUS);
                                    //上一个也没有值 返回 0
                                    if (beginValueInt == null)
                                    {
                                        beginValueInt = new IntData()
                                        {
                                            Id = point.Value.ID,
                                            Time = begin,
                                            Value = 0
                                        };
                                    }
                                    //上一个也没有值，就取下一个
                                    if (beginValueInt == null)
                                    {
                                        beginValueInt = his.GetIntSingleValue(point.Value.ID, begin, HistorianMode.NEXT);
                                    }
                                }
                                if (beginValueInt != null)
                                {
                                    beginValue = beginValueInt.Value;
                                }
                                IntData endValueInt = his.GetIntSingleValue(point.Value.ID, end, HistorianMode.EXACT);
                                if (endValueInt == null)
                                {
                                    endValueInt = his.GetIntSingleValue(point.Value.ID, end, HistorianMode.PREVIOUS);
                                    if (endValueInt == null)
                                    {
                                        endValueInt = his.GetIntSingleValue(point.Value.ID, begin, HistorianMode.NEXT);
                                    }

                                }
                                //去年
                                IntData beginValueIntly = his.GetIntSingleValue(point.Value.ID, lybegin, HistorianMode.EXACT);
                                if (beginValueIntly == null)
                                {
                                    //没有准确值，则取上一个值
                                    beginValueIntly = his.GetIntSingleValue(point.Value.ID, lybegin, HistorianMode.PREVIOUS);
                                    //上一个也没有值 返回 0
                                    if (beginValueIntly == null)
                                    {
                                        beginValueIntly = new IntData()
                                        {
                                            Id = point.Value.ID,
                                            Time = begin,
                                            Value = 0
                                        };
                                    }
                                    if (beginValueIntly == null)
                                    {
                                        beginValueIntly = his.GetIntSingleValue(point.Value.ID, lybegin, HistorianMode.NEXT);
                                    }
                                }
                                IntData endValueIntly = his.GetIntSingleValue(point.Value.ID, lyend, HistorianMode.EXACT);
                                if (endValueIntly == null)
                                {
                                    endValueIntly = his.GetIntSingleValue(point.Value.ID, lyend, HistorianMode.PREVIOUS);
                                    if (endValueIntly == null)
                                    {
                                        endValueIntly = his.GetIntSingleValue(point.Value.ID, lyend, HistorianMode.NEXT);
                                    }
                                }

                                if (beginValueInt != null && endValueInt != null)
                                {
                                    // hasvalue = true;
                                    summary.Difference = ((double)endValueInt.Value - (double)beginValueInt.Value);
                                    summary.StartTime = beginValueInt.Time.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                    summary.EndTime = endValueInt.Time.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                }

                                if (beginValueIntly != null && endValueIntly != null)
                                {
                                    lydifference = ((double)endValueIntly.Value - (double)beginValueIntly.Value);
                                }

                                break;
                            case DataType.REAL16:
                            case DataType.REAL32:
                            case DataType.REAL64:
                                //今年
                                FloatData beginValueFloat = his.GetFloatSingleValue(point.Value.ID, begin, HistorianMode.EXACT);
                                if (beginValueFloat == null)
                                {
                                    beginValueFloat = his.GetFloatSingleValue(point.Value.ID, begin, HistorianMode.PREVIOUS);
                                    //上一个也没有值 返回 0
                                    if (beginValueFloat == null)
                                    {
                                        beginValueFloat = new FloatData()
                                        {
                                            Id = point.Value.ID,
                                            Time = begin,
                                            Value = 0
                                        };
                                    }
                                    if (beginValueFloat == null)
                                    {
                                        beginValueFloat = his.GetFloatSingleValue(point.Value.ID, begin, HistorianMode.NEXT);
                                    }
                                }
                                if (beginValueFloat != null)
                                {
                                    beginValue = beginValueFloat.Value;
                                }
                                FloatData endValueFloat = his.GetFloatSingleValue(point.Value.ID, end, HistorianMode.EXACT);
                                if (endValueFloat == null)
                                {
                                    endValueFloat = his.GetFloatSingleValue(point.Value.ID, end, HistorianMode.PREVIOUS);
                                    if (endValueFloat == null)
                                    {
                                        endValueFloat = his.GetFloatSingleValue(point.Value.ID, end, HistorianMode.NEXT);
                                    }
                                }
                                if (beginValueFloat != null && endValueFloat != null)
                                {
                                    // hasvalue = true;
                                    summary.Difference = ((double)endValueFloat.Value - (double)beginValueFloat.Value);
                                    summary.StartTime = beginValueFloat.Time.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                    summary.EndTime = endValueFloat.Time.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                }
                                //去年
                                FloatData beginValueFloatly = his.GetFloatSingleValue(point.Value.ID, lybegin, HistorianMode.EXACT);
                                if (beginValueFloatly == null)
                                {

                                    beginValueFloatly = his.GetFloatSingleValue(point.Value.ID, lybegin, HistorianMode.PREVIOUS);
                                    //上一个也没有值 返回 0
                                    if (beginValueFloatly == null)
                                    {
                                        beginValueFloatly = new FloatData()
                                        {
                                            Id = point.Value.ID,
                                            Time = begin,
                                            Value = 0
                                        };
                                    }
                                    if (beginValueFloatly == null)
                                    {
                                        beginValueFloatly = his.GetFloatSingleValue(point.Value.ID, lybegin, HistorianMode.NEXT);
                                    }
                                }
                                FloatData endValueFloatly = his.GetFloatSingleValue(point.Value.ID, lyend, HistorianMode.EXACT);
                                if (endValueFloatly == null)
                                {
                                    endValueFloatly = his.GetFloatSingleValue(point.Value.ID, lyend, HistorianMode.PREVIOUS);
                                    if (endValueFloatly == null)
                                    {
                                        endValueFloatly = his.GetFloatSingleValue(point.Value.ID, lyend, HistorianMode.NEXT);
                                    }
                                }
                                if (beginValueFloatly != null && endValueFloatly != null)
                                {
                                    lydifference = ((double)endValueFloatly.Value - (double)beginValueFloatly.Value);
                                }

                                break;
                        }
                        Dictionary<string, List<DataEntity>> hisdata = History.Get(new string[] { point.Key }, beginTime, endTime);
                        if (hisdata[point.Key].Count != 0)
                        {
                            try
                            {
                                ese = his.GetNumberSummary(point.Value.ID, begin, end);
                            }
                            catch
                            {
                                ese = new SummaryEntity();
                            }
                            summary.Min = ese.Min;
                            summary.Max = ese.Max;
                            summary.Total = ese.Total;
                            summary.PowerAvg = ese.PowerAvg;
                            summary.CalcAvg = ese.CalcAvg;
                            try
                            {
                                var temp_count = his.GetNumberArchivedValuesCount(point.Value.ID, begin, end);
                                temp_count = his.GetFloatArchivedValues(point.Value.ID, temp_count, begin, end).Count;
                                if (temp_count >= 1 &&
                                    create_dates[i] >= begin &&
                                    create_dates[i] <= end)
                                {
                                    temp_count -= 1;
                                }
                                summary.CalcTotal = temp_count * ese.CalcAvg;
                            }
                            catch
                            {
                                summary.CalcTotal = 0;
                            }
                            if (lydifference != 0)
                            {
                                summary.EqLastYear = summary.Difference / lydifference * 100.0;
                            }
                            //可利用率(全部时间-故障时间)/全部时间  以s为单位
                            TimeSpan span = Convert.ToDateTime(endTime).Subtract(Convert.ToDateTime(beginTime));
                            if (span.TotalSeconds != 0)
                            {
                                double availble = (span.TotalSeconds - summary.Difference) / span.TotalSeconds;
                                summary.Availble = availble * 100;
                            }
                            else
                            {
                                summary.Availble = 0;
                            }
                        }
                        else
                        {
                            summary.Min = beginValue;
                            summary.Max = beginValue;
                            summary.Total = 0;
                            summary.PowerAvg = beginValue;
                            summary.CalcAvg = beginValue;
                            summary.Availble = 100;
                            summary.CalcTotal = 0;
                            if (lydifference != 0)
                            {
                                summary.EqLastYear = summary.Difference / lydifference * 100.0;
                            }
                        }
                        dicresult.TryAdd(point.Key, summary);
                    }
                    catch (Exception e)
                    {
                        //Log.Error("HistorySummary异常", e);
                    }
                }
                if (conn.IsPool)
                {
                    conn.Close();
                }
                else
                {
                    conn.Close();
                    conn.RealClose();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        public static Dictionary<string, object> GetSummary(
               string[] tagNames, string beginTime, string endTime)
        {
            Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();
            DateTime begin = Convert.ToDateTime(beginTime);
            DateTime end = Convert.ToDateTime(endTime);
            if (end > ConnectionPool.ServerTime)
            {
                end = ConnectionPool.ServerTime;
            }

            points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;

            TimeSpan span = Convert.ToDateTime(endTime).Subtract(Convert.ToDateTime(beginTime));
            var result = new Dictionary<string, object>();

            ParallelLoopResult LoopResult =
                Parallel.ForEach(points, item =>
                {
                    var conn = ConnectionPool.TryGetConnection();
                    try
                    {
                        var his = new HistorianImpl(conn);
                        switch (item.Value.Type)
                        {
                            case DataType.INT8:
                            case DataType.INT16:
                            case DataType.INT32:
                            case DataType.UINT8:
                            case DataType.UINT16:
                            case DataType.UINT32:
                            case DataType.INT64:
                            case DataType.BOOL:
                            {
                                var left = his.GetIntSingleValue(item.Value.ID, begin, HistorianMode.EXACT);
                                if (left == null)
                                    left = his.GetIntSingleValue(item.Value.ID, begin, HistorianMode.PREVIOUS);
                                var right = his.GetIntSingleValue(item.Value.ID, end, HistorianMode.EXACT);
                                if (right == null)
                                    right = his.GetIntSingleValue(item.Value.ID, end, HistorianMode.PREVIOUS);
                                if (left == null)
                                {
                                    left = new IntData()
                                    {
                                        Value = 0
                                    };
                                }
                                if (right == null)
                                {
                                    right = new IntData()
                                    {
                                        Value = 0
                                    };
                                }
                                var last_left = his.GetIntSingleValue(item.Value.ID, begin.AddYears(-1), HistorianMode.EXACT);
                                if (last_left == null)
                                    last_left = his.GetIntSingleValue(item.Value.ID, begin.AddYears(-1), HistorianMode.PREVIOUS);
                                var last_right = his.GetIntSingleValue(item.Value.ID, end.AddYears(-1), HistorianMode.EXACT);
                                if (last_right == null)
                                    last_right = his.GetIntSingleValue(item.Value.ID, end.AddYears(-1), HistorianMode.PREVIOUS);
                                if (last_left == null)
                                {
                                    last_left = new IntData()
                                    {
                                        Value = 0
                                    };
                                }
                                if (last_right == null)
                                {
                                    last_right = new IntData()
                                    {
                                        Value = 0
                                    };
                                }
                                var diff = right.Value - left.Value;
                                var last_diff = last_right.Value - last_left.Value;
                                result.Add(item.Key, new
                                {
                                    Difference = diff,
                                    EqLastYear = (double)diff / last_diff * 100,
                                    Availble = (span.TotalSeconds - diff) /
                                    span.TotalSeconds * 100.0
                                });
                            }
                            break;
                            case DataType.REAL16:
                            case DataType.REAL32:
                            case DataType.REAL64:
                            {
                                var left = his.GetFloatSingleValue(item.Value.ID, begin, HistorianMode.EXACT);
                                if (left == null)
                                    left = his.GetFloatSingleValue(item.Value.ID, begin, HistorianMode.PREVIOUS);
                                var right = his.GetFloatSingleValue(item.Value.ID, end, HistorianMode.EXACT);
                                if (right == null)
                                    right = his.GetFloatSingleValue(item.Value.ID, end, HistorianMode.PREVIOUS);
                                if (left == null)
                                {
                                    left = new FloatData()
                                    {
                                        Value = 0
                                    };
                                }
                                if (right == null)
                                {
                                    right = new FloatData()
                                    {
                                        Value = 0
                                    };
                                }
                                var last_left = his.GetFloatSingleValue(item.Value.ID, begin.AddYears(-1), HistorianMode.EXACT);
                                if (last_left == null)
                                    last_left = his.GetFloatSingleValue(item.Value.ID, begin.AddYears(-1), HistorianMode.PREVIOUS);
                                var last_right = his.GetFloatSingleValue(item.Value.ID, end.AddYears(-1), HistorianMode.EXACT);
                                if (last_right == null)
                                    last_right = his.GetFloatSingleValue(item.Value.ID, end.AddYears(-1), HistorianMode.PREVIOUS);
                                if (last_left == null)
                                {
                                    last_left = new FloatData()
                                    {
                                        Value = 0
                                    };
                                }
                                if (last_right == null)
                                {
                                    last_right = new FloatData()
                                    {
                                        Value = 0
                                    };
                                }
                                var diff = right.Value - left.Value;
                                var last_diff = last_right.Value - last_left.Value;
                                result.Add(item.Key, new
                                {
                                    Difference = diff,
                                    EqLastYear = (double)diff / last_diff * 100,
                                    Availble = (span.TotalSeconds - diff) /
                                    span.TotalSeconds * 100.0
                                });
                            }
                            break;
                        }
                        if (conn.IsPool)
                        {
                            conn.Close();
                        }
                        else
                        {
                            conn.Close();
                            conn.RealClose();
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        if (conn != null)
                        {
                            conn.Close();
                        }
                    }
                });
            return result;
        }

        /// <summary>
        /// 日，月，年，统计值请求接口
        /// </summary>
        /// <param name="tags">点名</param>
        /// <param name="times">时间</param>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static List<List<object>> GetSummaryByTime(
            string[] tags, string[] times, string type)
        {
            List<List<object>> res = new List<List<object>>();
            if (times.Length != tags.Length)
            {
                return res;
            }
            List<DateTime> list_dt = new List<DateTime>();
            try
            {
                foreach (var time in times)
                {
                    list_dt.Add(Convert.ToDateTime(time));
                }
            }
            catch (FormatException)
            {
                return res;
            }
            List<Dictionary<DateTime, DateTime>> dic_dt =
                new List<Dictionary<DateTime, DateTime>>();

            if (type.ToLower() == "year")
            {
                foreach (var dt in list_dt)
                {
                    Dictionary<DateTime, DateTime> dic_temp =
                        new Dictionary<DateTime, DateTime>();
                    for (int i = 1; i <= 12; i++)
                    {
                        dic_temp.Add(
                          new DateTime(dt.Year, i, 1, 0, 0, 0),
                          new DateTime(dt.Year, i, DateTime.DaysInMonth(dt.Year, i), 23, 59, 59));
                    }
                    dic_dt.Add(dic_temp);
                }
            }
            else if (type.ToLower() == "month")
            {
                foreach (var dt in list_dt)
                {
                    Dictionary<DateTime, DateTime> dic_temp =
                        new Dictionary<DateTime, DateTime>();
                    for (int i = 1; i <= DateTime.DaysInMonth(dt.Year, dt.Month); i++)
                    {
                        dic_temp.Add(
                            new DateTime(dt.Year, dt.Month, i, 0, 0, 0),
                            new DateTime(dt.Year, dt.Month, i, 23, 59, 59));
                    }
                    dic_dt.Add(dic_temp);
                }
            }
            else if (type.ToLower() == "day")
            {
                foreach (var dt in list_dt)
                {
                    Dictionary<DateTime, DateTime> dic_temp =
                        new Dictionary<DateTime, DateTime>();
                    for (int i = 0; i <= 23; i++)
                    {
                        dic_temp.Add(
                        new DateTime(dt.Year, dt.Month, dt.Day, i, 0, 0),
                        new DateTime(dt.Year, dt.Month, dt.Day, i, 59, 59));
                    }
                    dic_dt.Add(dic_temp);
                }
            }
            else
            {
                return res;
            }
            List<string> need_search_tags = new List<string>();
            foreach (var tag in tags)
            {
                if (!need_search_tags.Contains(tag))
                {
                    need_search_tags.Add(tag);
                }
            }
            var temp_points = TagPointManager.GetPointInfosByTagNames(
                                              need_search_tags.ToArray());
            List<KeyValuePair<string, int>> points = new List<KeyValuePair<string, int>>();
            foreach (var tag in tags)
            {
                if (temp_points.ContainsKey(tag))
                {
                    points.Add(new KeyValuePair<string, int>(tag, temp_points[tag].ID));
                }
            }
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                HistorianImpl his = new HistorianImpl(conn);
                int index = 0;
                foreach (var point in points)
                {
                    var cur_dt = dic_dt[index];
                    var temp_list = new List<object>();
                    res.Add(temp_list);
                    foreach (var temp_dt in cur_dt)
                    {
                        if (temp_dt.Key > DateTime.Now ||
                            temp_dt.Value > DateTime.Now)
                        {
                            temp_list.Add(new
                            {
                                CalcAvg = 0,
                                Count = 0,
                                EndTime = temp_dt.Value,
                                Error = 0,
                                Max = 0,
                                Min = 0,
                                PowerAvg = 0,
                                StartTime = temp_dt.Key,
                                Total = 0,
                                Diff = 0
                            });
                        }
                        else
                        {
                            double diff = 0;

                            try
                            {
                                var temp_left = History.Get(
                                    new string[] { point.Key }, temp_dt.Key);
                                var temp_right = History.Get(
                                    new string[] { point.Key }, temp_dt.Value);
                                if (temp_left.ContainsKey(point.Key) &&
                                    temp_right.ContainsKey(point.Key) &&
                                    double.TryParse(temp_left[point.Key].Value, out double l) &&
                                    double.TryParse(temp_right[point.Key].Value, out double r))
                                {
                                    diff = r - l;
                                }
                            }
                            catch
                            {

                            }

                            try
                            {
                                var summary = his.GetNumberSummary(point.Value,
                                  temp_dt.Key, temp_dt.Value);
                                temp_list.Add(new
                                {
                                    summary.CalcAvg,
                                    summary.Count,
                                    summary.EndTime,
                                    summary.Error,
                                    summary.Max,
                                    summary.Min,
                                    summary.PowerAvg,
                                    summary.StartTime,
                                    summary.Total,
                                    Diff = diff
                                });
                            }
                            catch (Exception)
                            {
                                temp_list.Add(new
                                {
                                    CalcAvg = 0,
                                    Count = 0,
                                    EndTime = temp_dt.Value,
                                    Error = 0,
                                    Max = 0,
                                    Min = 0,
                                    PowerAvg = 0,
                                    StartTime = temp_dt.Key,
                                    Total = 0,
                                    Diff = diff
                                });
                            }

                        }
                    }
                    index++;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
            return res;
        }

    }
}
