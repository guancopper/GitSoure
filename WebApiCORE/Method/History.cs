using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using GoldenAPI;
using GoldenAPI.Common;
using GoldenAPI.Impl;
using GoldenAPI.Inter;
using GoldenAPI.Model.Base;
using GoldenAPI.Model.Data;
using GoldenAPI.Util;
using WebApICore.Base;
using WebAPICore.Struct;

namespace WebAPICore.Method
{
    public static class History
    {

        public static Dictionary<string, List<DataEntity>> Get(string[] tagNames, string beginTime, string endTime)
        {
            Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();
            DateTime begin = DateTime.Now;
            DateTime end = DateTime.Now;
            if (string.IsNullOrEmpty(beginTime))
            {
                begin = DateTime.Now.AddMinutes(-10);
            }
            else
            {
                begin = Convert.ToDateTime(beginTime);
            }

            if (string.IsNullOrEmpty(endTime))
            {
                end = DateTime.Now;
            }
            else
            {
                end = Convert.ToDateTime(endTime);
            }
            points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                return GetHistoryData(points, begin, end);
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

        public static Dictionary<string, List<DataEntity>> Get(string[] tagNames, string[] filters, string beginTime, string endTime)
        {
            Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();
            DateTime begin = DateTime.Now;
            DateTime end = DateTime.Now;
            begin = Convert.ToDateTime(beginTime);
            end = Convert.ToDateTime(endTime);
            if (tagNames.Length <= 0) return null;
            if (filters.Length <= 0) return null;
            points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;
            return GetFilterHistoryData(points, begin, end, filters);
        }

        public static EntityResult Post(List<PointData> data)
        {
            List<PointData> nonExistList = new List<PointData>();
            Dictionary<DataType, Dictionary<int, List<PointData>>> dataCollection = new Dictionary<DataType, Dictionary<int, List<PointData>>>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                dataCollection = MapPointData(data, out nonExistList);
                EntityResult result = PutData(dataCollection, nonExistList);
                return result;
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

        public static bool PostSingle(PointData data, bool isSingleData)
        {
            List<PointData> nonExistList = new List<PointData>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                Dictionary<DataType, Dictionary<int, List<PointData>>> dataCollection =
MapPointData(new List<PointData>() { data }, out nonExistList);
                if (dataCollection == null || dataCollection.Count <= 0)
                {
                    return false;
                }
                EntityResult result = PutData(dataCollection, nonExistList);
                if (result.SuccessCount == 0)
                {
                    string errorMsg = string.Format("错误码为：{0}，错误信息：{1}", result.ErrorList[0].Error,
                        result.ErrorList[0].ErrorMsg);
                    return false;
                }
                return true;
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

        public static bool Delete(string[] tags, string beginTime, string endTime)
        {
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                DateTime begin = Convert.ToDateTime(beginTime);
                DateTime end = Convert.ToDateTime(endTime);

                Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();

                List<int> ids = new List<int>();
                foreach (var tag in tags)
                {
                    if (TagPointManager.PointNameDictionary.ContainsKey(tag))
                    {
                        ids.Add(TagPointManager.PointNameDictionary[tag]);
                    }
                }
                foreach (var id in ids)
                {
                    if (TagPointManager.PointDictionary.ContainsKey(id))
                    {
                        var point = TagPointManager.PointDictionary[id];
                        points.Add(point.TagName, new PointInfo()
                        {
                            ID = point.ID,
                            Type = point.DataType
                        });
                    }
                }
                if (points.Count <= 0) return false;
                DeleteHistoryData(points, begin, end);
                return true;
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

        public static Dictionary<string, DataEntity> Get(string[] tagNames,
            DateTime time)
        {
            Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();

            points = TagPointManager.GetPointInfosByTagNames(tagNames);
            if (points.Count <= 0) return null;

            var result = new Dictionary<string, DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);

                foreach (var item in points)
                {
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
                            var temp = his.GetIntSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetIntSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Ms = temp.Ms,
                                Value = temp.Value.ToString()
                            });
                        }
                        break;
                        case DataType.REAL16:
                        case DataType.REAL32:
                        case DataType.REAL64:
                        {
                            var temp = his.GetFloatSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetFloatSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Ms = temp.Ms,
                                Value = temp.Value.ToString()
                            });
                        }
                        break;
                        case DataType.STRING:
                        {
                            var temp = his.GetStringSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetStringSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Ms = temp.Ms,
                                Value = temp.Value.ToString()
                            });
                        }
                        break;
                        case DataType.DateTime:
                        {
                            var temp = his.GetDateTimeSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetDateTimeSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Value = temp.Value.ToString()
                            });
                        }
                        break;
                        case DataType.COOR:
                        {
                            var temp = his.GetCoorSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetCoorSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Ms = temp.Ms,
                                Value = temp.ToString()
                            });
                        }
                        break;
                        case DataType.BLOB:
                        {
                            var temp = his.GetBlobSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.EXACT);
                            if (temp == null)
                                temp = his.GetBlobSingleValue(item.Value.ID, time, GoldenAPI.Model.Historian.HistorianMode.PREVIOUS);
                            if (temp == null)
                                continue;
                            result.Add(item.Key, new DataEntity
                            {
                                DateTime = temp.Time,
                                Ms = temp.Ms,
                                Value = string.Join(string.Empty, temp.Value.Select(tempByte => tempByte.ToString("D3"))),
                            });
                        }
                        break;
                        default:
                        {

                        }
                        break;
                    }
                }

                return result;
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

        public static Dictionary<string, DataEntity> Get(string[] tagNames,
            string datetime)
        {
            DateTime time = DateTime.Now;
            if (string.IsNullOrEmpty(datetime))
            {
                return new Dictionary<string, DataEntity>();
            }
            else
            {
                time = Convert.ToDateTime(datetime);
            }
            return Get(tagNames, time);
        }

        #region [ 获取历史数据 ]

        private static Dictionary<string, List<DataEntity>> GetHistoryData(Dictionary<string, PointInfo> namePointDic, DateTime startTime, DateTime endTime)
        {
            var result = new Dictionary<string, List<DataEntity>>();
            foreach (KeyValuePair<string, PointInfo> item in namePointDic)
            {
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
                        result.Add(item.Key, GetIntHistory(item.Value.ID, startTime, endTime, string.Empty));
                        break;
                    case DataType.REAL16:
                    case DataType.REAL32:
                    case DataType.REAL64:
                        result.Add(item.Key, GetFloatHistory(item.Value.ID, startTime, endTime, string.Empty));
                        break;
                    case DataType.STRING:
                    {
                        result.Add(item.Key, GetStringHistory(item.Value.ID, startTime, endTime));
                    }
                    break;
                    case DataType.DateTime:
                    {
                        result.Add(item.Key, GetDateTimeHistory(item.Value.ID, startTime, endTime));
                    }
                    break;
                    case DataType.COOR:
                    {
                        result.Add(item.Key, GetCoorTimeHistory(item.Value.ID, startTime, endTime));
                    }
                    break;
                    case DataType.BLOB:
                    {
                        result.Add(item.Key, GetBlobHistory(item.Value.ID, startTime, endTime));
                    }
                    break;
                    default:
                    {

                    }
                    break;
                }
            }
            return result;
        }

        private static List<DataEntity> GetIntHistory(int id, DateTime startTime, DateTime endTime, string filter)
        {
            Entity<IntData> data;
            List<DataEntity> list = new List<DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;
                bool isFlter = !string.IsNullOrEmpty(filter);
                data = isFlter ? his.GetIntArchivedValuesFilt(id, filter, count, startTime, endTime) : his.GetIntArchivedValues(id, count, startTime, endTime);
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
            if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

            var temp = data.Data;

            foreach (var item in data.Data)
            {
                var entity = new DataEntity
                {
                    // entity.Id = item.Id;
                    // Time = item.Time + "." + item.Ms,
                    DateTime = item.Time,
                    Ms = item.Ms,
                    Value = item.Value.ToString()
                };
                list.Add(entity);
            }
            return list;
        }

        private static List<DataEntity> GetFloatHistory(int id, DateTime startTime, DateTime endTime, string filter)
        {
            Entity<FloatData> data;
            var list = new List<DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;

                bool isFlter = !string.IsNullOrEmpty(filter);
                data = isFlter ? his.GetFloatArchivedValuesFilt(id, filter, count, startTime, endTime) : his.GetFloatArchivedValues(id, count, startTime, endTime);
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
            if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

            list.AddRange(data.Data.Select(item => new DataEntity()
            {
                // entity.Id = item.Id;
                // Time = item.Time + "." + item.Ms,
                DateTime = item.Time,
                Ms = item.Ms,
                Value = item.Value.ToString()
            }));
            return list;
        }

        private static List<DataEntity> GetStringHistory(int id, DateTime startTime, DateTime endTime)
        {
            Entity<StringData> data;
            var list = new List<DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;
                data = his.GetStringArchiveValues(id, count, startTime, endTime);
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
            if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

            list.AddRange(data.Data.Select(item => new DataEntity()
            {
                // entity.Id = item.Id;
                // Time = item.Time + "." + item.Ms,
                DateTime = item.Time,
                Ms = item.Ms,
                Value = item.Value.ToString()
            }));
            return list;
        }

        private static List<DataEntity> GetDateTimeHistory(int id, DateTime startTime, DateTime endTime)
        {
            Entity<DateTimeData> data;
            var list = new List<DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;
                data = his.GetArchivedDateTimeValues(id, count, startTime, endTime);
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
            if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

            list.AddRange(data.Data.Select(item => new DataEntity()
            {
                //Id = item.Id,
                DateTime = item.Time,
                //Time = item.Time.ToString(),
                Value = item.Value.ToString(),
            }));
            return list;
        }

        private static List<DataEntity> GetCoorTimeHistory(int id, DateTime startTime, DateTime endTime)
        {
            var list = new List<DataEntity>();
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;

                Entity<CoorData> data = his.GetCoorArchivedValues(id, count, startTime, endTime);

                if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

                list.AddRange(data.Data.Select(item => new DataEntity()
                {
                    //Id = item.Id,
                    DateTime = item.Time,
                    //Time = item.Time + "." + item.Ms,
                    Value = item.ToString(),
                }));
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
            return list;

        }

        private static List<DataEntity> GetBlobHistory(int id, DateTime startTime, DateTime endTime)
        {
            var list = new List<DataEntity>();
            Entity<BlobData> data;
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                int count = his.GetNumberArchivedValuesCount(id, startTime, endTime);
                if (count <= 0) return list;
                data = his.GetBlobArchiveValues(id, count, startTime, endTime);
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
            if (data == null || data.Count <= 0 || data.Data.Count <= 0) return list;

            list.AddRange(data.Data.Select(item => new DataEntity()
            {
                //Id = item.Id,
                DateTime = item.Time,
                //Time = item.Time + "." + item.Ms,
                Value = string.Join(string.Empty, item.Value.Select(tempByte => tempByte.ToString("D3"))),
            }));
            return list;
        }

        #endregion

        #region [ 根据过滤条件获取历史数据 ]

        private static Dictionary<string, List<DataEntity>> GetFilterHistoryData(Dictionary<string, PointInfo> namePointDic,
            DateTime startTime, DateTime endTime, string[] filter)
        {
            var result = new Dictionary<string, List<DataEntity>>();
            var index = 0;
            foreach (KeyValuePair<string, PointInfo> item in namePointDic)
            {
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
                        result.Add(item.Key, GetIntHistory(item.Value.ID, startTime, endTime, filter[index]));
                        break;
                    case DataType.REAL16:
                    case DataType.REAL32:
                    case DataType.REAL64:
                        result.Add(item.Key, GetFloatHistory(item.Value.ID, startTime, endTime, filter[index]));
                        break;
                    default:
                    {

                    }
                    break;
                }
                index++;
            }
            return result;
        }

        #endregion

        #region [ 删除历史数据 ]

        private static void DeleteHistoryData(Dictionary<string, PointInfo> namePointDic, DateTime startTime, DateTime endTime)
        {
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var his = new HistorianImpl(conn);
                foreach (KeyValuePair<string, PointInfo> item in namePointDic)
                {
                    his.RemoveValues(item.Value.ID, startTime, endTime);
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

        #endregion

        #region [ 写入历史数据 ]

        private static Dictionary<DataType, Dictionary<int, List<PointData>>> MapPointData(List<PointData> data, out List<PointData> nonExistList)
        {
            var dic = new Dictionary<DataType, Dictionary<int, List<PointData>>>();
            List<PointData> pList = new List<PointData>();
            foreach (var item in data)
            {
                int id;
                PointDataFX point = null;
                if (TagPointManager.PointNameDictionary.ContainsKey(item.TagName))
                {
                    id = TagPointManager.PointNameDictionary[item.TagName];
                    if (TagPointManager.PointDictionary.ContainsKey(id))
                    {
                        point = TagPointManager.PointDictionary[id];
                    }
                }
                if (point == null)
                {
                    PointData pData = new PointData
                    {
                        ID = 0,
                        TagName = item.TagName,
                        Time = DateTime.Now.ToString("yyyy-MM-dd"),
                        Value = "",
                        Error = 0,
                        ErrorMsg = "测点全名称为:" + item.TagName + "；错误码为:0；错误信息为:指定的测点不存在！"
                    };
                    pList.Add(pData);
                }
                else
                {
                    if (dic.ContainsKey(point.DataType))
                    {
                        if (dic[point.DataType].Keys.Contains(point.ID))
                        {
                            dic[point.DataType][point.ID].Add(item);
                        }
                        else
                        {
                            List<PointData> pointData = new List<PointData>
                            {
                                item
                            };
                            dic[point.DataType].Add(point.ID, pointData);
                        }
                    }
                    else
                    {
                        var idNameCollection = new Dictionary<int, List<PointData>>();
                        List<PointData> pointData = new List<PointData>
                        {
                            item
                        };
                        idNameCollection.Add(point.ID, pointData);
                        dic.Add(point.DataType, idNameCollection);
                    }
                }
            }
            nonExistList = pList;
            return dic;
        }

        private static EntityResult PutData(Dictionary<DataType, Dictionary<int, List<PointData>>> dataCollection, List<PointData> nonExistList)
        {
            var result = new EntityResult();
            EntityResult tempResult = null;
            if (dataCollection == null || dataCollection.Count <= 0) return result;

            foreach (var item in dataCollection)
            {
                switch (item.Key)
                {
                    case DataType.INT8:
                    case DataType.INT16:
                    case DataType.INT32:
                    case DataType.UINT8:
                    case DataType.UINT16:
                    case DataType.UINT32:
                    case DataType.INT64:
                        tempResult = PutIntHistorianData(item.Value, false);
                        break;
                    case DataType.REAL16:
                    case DataType.REAL32:
                    case DataType.REAL64:
                        tempResult = PutDoubleHistorianData(item.Value);
                        EntityResult res = tempResult;
                        break;
                    case DataType.BOOL:
                        tempResult = PutIntHistorianData(item.Value, true);
                        break;
                    case DataType.STRING:
                        tempResult = PutStringHistorianData(item.Value);
                        break;
                    case DataType.DateTime:
                        tempResult = PutDatetimeHistorianData(item.Value);
                        break;
                    case DataType.COOR:
                        tempResult = PutCoorHistorianData(item.Value);
                        break;
                    case DataType.BLOB:
                        tempResult = PutBlobHistorianData(item.Value);
                        break;
                }

                //合并未写入成功的
                CombineEntityResult(result, tempResult);
            }
            if (nonExistList.Count > 0)
            {
                foreach (var item in nonExistList)
                {
                    result.ErrorList.Add(item);
                }
            }
            return result;
        }

        private static EntityResult PutIntHistorianData(Dictionary<int, List<PointData>> data, bool isBool)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }
            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var states = new List<long>();
            var quelities = new List<short>();
            var values = new double[count];
            var errors = new uint[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var pointData in item.Value)
                {
                    var time = Convert.ToDateTime(pointData.Time);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    long value = isBool ? GetBoolValue(pointData.Value) : Convert.ToInt64(pointData.Value);
                    states.Add(value);
                    //quelities.Add((short)item.Value.Quality);
                    tableTagNames.Add(pointData.TagName);
                }

            }
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = HistorianFunctions.PutArchivedValues(conn.Handler, out count, ids.ToArray(),
                datetimes.ToArray(), mses.ToArray(), values, states.ToArray(), quelities.ToArray(), errors);
                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            return BuildResultEntity(data, errors, count);
        }

        private static int GetBoolValue(string value)
        {
            int result;

            if (string.Compare(value, "true", System.StringComparison.OrdinalIgnoreCase) == 0)
            {
                result = 1;
            }
            else if (string.Compare(value, "false", System.StringComparison.OrdinalIgnoreCase) == 0)
            {
                result = 0;
            }
            else
            {
                result = Convert.ToInt32(value);
            }
            return result;
        }

        private static EntityResult PutDoubleHistorianData(Dictionary<int, List<PointData>> data)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }
            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var values = new List<double>();
            var quelities = new List<short>();
            var errors = new uint[count];
            var states = new long[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var pointData in item.Value)
                {
                    var time = Convert.ToDateTime(pointData.Time);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    values.Add(Convert.ToDouble(pointData.Value));
                    tableTagNames.Add(pointData.TagName);
                }
            }

            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = HistorianFunctions.PutArchivedValues(conn.Handler, out count, ids.ToArray(), datetimes.ToArray(), mses.ToArray(), values.ToArray(), states, quelities.ToArray(), errors);

                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            return BuildResultEntity(data, errors, count);
        }

        private static EntityResult PutDatetimeHistorianData(Dictionary<int, List<PointData>> data)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }
            int needCount = count;
            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var quelities = new List<short>();
            var intPtrs = new List<IntPtr>();
            var lens = new List<short>();
            var errors = new uint[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var pointData in item.Value)
                {
                    var time = Convert.ToDateTime(pointData.Time);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    lens.Add((short)pointData.Value.Length);
                    intPtrs.Add(Marshal.StringToHGlobalAnsi(Convert.ToDateTime(pointData.Value).ToString("yyyy-MM-dd HH:mm:ss.fff")));

                    tableTagNames.Add(pointData.TagName);
                }

            }
            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = HistorianFunctions.PutArchivedDateTimeValues(conn.Handler, out count, ids.ToArray(),
                  datetimes.ToArray(), mses.ToArray(), intPtrs.ToArray(), lens.ToArray(), quelities.ToArray(), errors);
                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            foreach (var item in intPtrs)
            {
                Marshal.FreeCoTaskMem(item);
            }
            return BuildResultEntity(data, errors, count);
        }

        private static EntityResult PutStringHistorianData(Dictionary<int, List<PointData>> data)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }
            int needCount = count;

            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var quelities = new List<short>();
            var intPtrs = new List<IntPtr>();
            var lens = new List<short>();
            var errors = new uint[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var pointData in item.Value)
                {
                    var time = Convert.ToDateTime(pointData.Time);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    // quelities.Add((short)item.Value.Quality);

                    lens.Add((short)pointData.Value.Length);
                    intPtrs.Add(Marshal.StringToHGlobalAnsi(pointData.Value));

                    tableTagNames.Add(pointData.TagName);
                }

            }


            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = HistorianFunctions.PutArchivedBlobValues(conn.Handler, out count, ids.ToArray(),
                  datetimes.ToArray(), mses.ToArray(), intPtrs.ToArray(), lens.ToArray(), quelities.ToArray(), errors);

                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            foreach (var item in intPtrs)
            {
                Marshal.FreeCoTaskMem(item);
            }
            return BuildResultEntity(data, errors, count);
        }

        private static EntityResult PutBlobHistorianData(Dictionary<int, List<PointData>> data)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }
            int needCount = count;

            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var quelities = new List<short>();
            var intPtrs = new List<IntPtr>();
            var lens = new List<short>();
            var errors = new uint[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var pointData in item.Value)
                {
                    var time = Convert.ToDateTime(pointData.Value);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    //  quelities.Add((short)item.Value.Quality);

                    lens.Add((short)pointData.Value.Length);
                    intPtrs.Add(Marshal.StringToHGlobalAnsi(pointData.Value));

                    tableTagNames.Add(pointData.TagName);
                }

            }


            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = result = HistorianFunctions.PutArchivedBlobValues(conn.Handler, out count, ids.ToArray(),
                  datetimes.ToArray(), mses.ToArray(), intPtrs.ToArray(), lens.ToArray(), quelities.ToArray(), errors);

                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            foreach (var item in intPtrs)
            {
                Marshal.FreeCoTaskMem(item);
            }
            return BuildResultEntity(data, errors, count);
        }

        private static EntityResult PutCoorHistorianData(Dictionary<int, List<PointData>> data)
        {
            int count = 0;
            foreach (var item in data)
            {
                count += item.Value.Count;
            }

            int needCount = count;

            var ids = new List<int>();
            var datetimes = new List<int>();
            var mses = new List<short>();
            var x = new List<float>();
            var y = new List<float>();
            var quelities = new List<short>();
            var errors = new uint[count];
            var tableTagNames = new List<string>();
            foreach (var item in data)
            {
                foreach (var dataPoint in item.Value)
                {
                    var time = Convert.ToDateTime(dataPoint.Time);
                    ids.Add(item.Key);
                    datetimes.Add(RTDBConvert.ConvertToSeconds(time));
                    mses.Add((short)time.Millisecond);
                    // quelities.Add((short)item.Value.Quality);
                    x.Add(GetCoorValue(dataPoint.Value, "X"));
                    y.Add(GetCoorValue(dataPoint.Value, "Y"));

                    tableTagNames.Add(dataPoint.TagName);
                }

            }

            var conn = ConnectionPool.TryGetConnection();
            try
            {
                uint result = HistorianFunctions.PutArchivedCoorValues(conn.Handler, out count, ids.ToArray(),
                datetimes.ToArray(), mses.ToArray(), x.ToArray(), y.ToArray(), quelities.ToArray(), errors);

                if (result != 0)
                {
                    throw new RTDBException(RTDBError.GetError(result));
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
            return BuildResultEntity(data, errors, count);
        }

        private static float GetCoorValue(string value, string text)
        {
            var array = value.Split(',');
            if (text.Equals("X"))
            {
                return float.Parse(array[0].Replace("X:", string.Empty));
            }
            else
            {
                return float.Parse(array[1].Replace("Y:", string.Empty));
            }
        }

        private static EntityResult BuildResultEntity(Dictionary<int, List<PointData>> data, uint[] errors, int count)
        {
            var entityResult = new EntityResult
            {
                SuccessCount = count
            };
            if (count != data.Count)
            {
                int index = 0;
                var errorData = entityResult.ErrorList;
                foreach (var item in data)
                {
                    foreach (var pointData in item.Value)
                    {
                        if (errors[index] != 0)
                        {
                            pointData.Error = errors[index];
                            pointData.ErrorMsg = RTDBError.GetError(errors[index]);
                            errorData.Add(pointData);
                        }
                        index++;
                    }
                }
            }
            return entityResult;
        }

        private static EntityResult CombineEntityResult(EntityResult first, EntityResult second)
        {
            if (first.ErrorList == null)
            {
                second.SuccessCount += first.SuccessCount;
                return second;
            }
            if (second.ErrorList == null)
            {
                first.SuccessCount += second.SuccessCount;
                return first;
            }
            first.SuccessCount += second.SuccessCount;
            first.ErrorList.AddRange(second.ErrorList);
            return first;
        }

        #endregion

    }
}
