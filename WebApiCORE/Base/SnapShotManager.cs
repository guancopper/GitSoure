using GoldenAPI;
using GoldenAPI.Common;
using GoldenAPI.Impl;
using GoldenAPI.Model.Base;
using GoldenAPI.Model.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using WebAPICore.Struct;

namespace WebApICore.Base
{

    public static class SnapShotManager

    {

        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryInt
        { get; set; } = null;
        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryFloat
        { get; set; } = null;
        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryString
        { get; set; } = null;
        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryBlob
        { get; set; } = null;
        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryCoor
        { get; set; } = null;
        private static ConcurrentDictionary<int, PointDataFX> PointDictionaryDateTime
        { get; set; } = null;

        //private static bool EXIT = false;
        //private static bool NEWDATA = false;

        private static void Polling()
        {
            Stopwatch watch = new Stopwatch();
            while (true)
            {
                Thread.Sleep(1);
                if (!(!TagPointManager.InitDone || watch.Elapsed < new TimeSpan(0, 0, 0, 0, 500)))
                    continue;
                try
                {
                    watch.Restart();
                    List<int> int_input = null;
                    List<int> float_input = null;
                    List<int> string_input = null;
                    List<int> coor_input = null;
                    List<int> blob_input = null;
                    if (PointDictionaryInt != null && PointDictionaryInt.Count > 0)
                        int_input = PointDictionaryInt.Keys.ToList();
                    if (PointDictionaryString != null && PointDictionaryString.Count > 0)
                        string_input = PointDictionaryString.Keys.ToList();
                    if (PointDictionaryBlob != null && PointDictionaryBlob.Count > 0)
                        blob_input = PointDictionaryBlob.Keys.ToList();
                    if (PointDictionaryCoor != null && PointDictionaryCoor.Count > 0)
                        coor_input = PointDictionaryCoor.Keys.ToList();
                    if (PointDictionaryFloat != null && PointDictionaryFloat.Count > 0)
                        float_input = PointDictionaryFloat.Keys.ToList();

                    GetSnapShot(int_input, float_input, string_input, coor_input, blob_input,
                        out var int_list, out var string_list, out var blob_list, out var coor_list, out var float_list);

                    if (int_list != null)
                        foreach (var point in int_list.Data)
                        {
                            if (PointDictionaryInt.ContainsKey(point.Id))
                            {
                                PointDictionaryInt[point.Id].Error = point.Error;
                                PointDictionaryInt[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryInt[point.Id].Quality = point.Qualitie;
                                PointDictionaryInt[point.Id].Value = point.Value.ToString();
                                PointDictionaryInt[point.Id].DateTime = point.Time;
                            }
                        }
                    if (float_list != null)
                        foreach (var point in float_list.Data)
                        {
                            if (PointDictionaryFloat.ContainsKey(point.Id))
                            {
                                PointDictionaryFloat[point.Id].Error = point.Error;
                                PointDictionaryFloat[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryFloat[point.Id].Quality = point.Qualitie;
                                PointDictionaryFloat[point.Id].Value = point.Value.ToString();
                                PointDictionaryFloat[point.Id].DateTime = point.Time;
                            }
                        }
                    if (string_list != null)
                        foreach (var point in string_list.Data)
                        {
                            if (PointDictionaryString.ContainsKey(point.Id))
                            {
                                PointDictionaryString[point.Id].Error = point.Error;
                                PointDictionaryString[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryString[point.Id].Quality = point.Qualitie;
                                PointDictionaryString[point.Id].Value = point.Value;
                                PointDictionaryString[point.Id].DateTime = point.Time;
                            }
                        }
                    if (blob_list != null)
                        foreach (var point in blob_list.Data)
                        {
                            if (PointDictionaryBlob.ContainsKey(point.Id))
                            {
                                PointDictionaryBlob[point.Id].Error = point.Error;
                                PointDictionaryBlob[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryBlob[point.Id].Quality = point.Qualitie;
                                PointDictionaryBlob[point.Id].Value = string.Join(string.Empty, point.Value.Select(tempByte => tempByte.ToString("D3")));
                                PointDictionaryBlob[point.Id].DateTime = point.Time;
                            }
                        }
                    if (coor_list != null)
                        foreach (var point in coor_list.Data)
                        {
                            if (PointDictionaryCoor.ContainsKey(point.Id))
                            {
                                PointDictionaryCoor[point.Id].Error = point.Error;
                                PointDictionaryCoor[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryCoor[point.Id].Quality = point.Qualitie;
                                PointDictionaryCoor[point.Id].Value =
                                    "X:" + point.X + ",Y:" + point.Y;
                                PointDictionaryCoor[point.Id].DateTime = point.Time;
                            }
                        }
                    /*
                    if (datetime_list != null)
                        foreach (var point in datetime_list.Data)
                        {
                            if (PointDictionaryDateTime.ContainsKey(point.Id))
                            {
                                PointDictionaryDateTime[point.Id].Error = point.Error;
                                PointDictionaryDateTime[point.Id].ErrorMsg = RTDBError.GetError(point.Error);
                                PointDictionaryDateTime[point.Id].Quality = point.Qualitie;
                                PointDictionaryDateTime[point.Id].Value = point.Value.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                PointDictionaryDateTime[point.Id].DateTime = point.Time;
                            }
                        }
                     */
                    //NEWDATA = false;
                }
                catch (Exception e)
                {
                    //Log.Error("快照缓存池轮询异常", e);
                }
            }
        }

        private static void TagRemovePolling()
        {
            while (true)
            {
                if (TagPointManager.InitDone)
                {
                    CheckAccessTime(PointDictionaryInt);
                    CheckAccessTime(PointDictionaryFloat);
                    CheckAccessTime(PointDictionaryString);
                    CheckAccessTime(PointDictionaryBlob);
                    CheckAccessTime(PointDictionaryCoor);
                    CheckAccessTime(PointDictionaryDateTime);
                }
                Thread.Sleep(5000);
            }
        }

        private static void GetSnapShot(List<int> int_input, List<int> float_input, List<int> string_input, List<int> coor_input, List<int> blob_input, out Entity<IntData> int_list, out Entity<StringData> string_list, out Entity<BlobData> blob_list, out Entity<CoorData> coor_list, out Entity<FloatData> float_list)
        {
            int_list = null;
            string_list = null;
            blob_list = null;
            coor_list = null;
            float_list = null;
            //Entity<DateTimeData> datetime_list;
            var Connection = ConnectionPool.TryGetConnection();
            try
            {
                SnapshotImpl snapshot = new SnapshotImpl(Connection);
                if (int_input != null && int_input.Count > 0)
                    int_list = snapshot.GetIntSnapshots(int_input.ToArray());
                if (string_input != null && string_input.Count > 0)
                    string_list = snapshot.GetStringSnapshots(string_input.ToArray());
                if (blob_input != null && blob_input.Count > 0)
                    blob_list = snapshot.GetBlobSnapshots(blob_input.ToArray());
                if (coor_input != null && coor_input.Count > 0)
                    coor_list = snapshot.GetCoorSnapshots(coor_input.ToArray());
                if (float_input != null && float_input.Count > 0)
                    float_list = snapshot.GetFloatSnapshots(float_input.ToArray());
                //datetime_list = snapshot.GetDateTimeSnapshots(PointDictionaryDateTime.Keys.ToArray());

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (Connection != null)
                {
                    Connection.Close();
                }
            }

        }

        /// <summary>
        /// 判断上次访问快照的时间
        /// </summary>
        /// <param name="dic"></param>
        private static void CheckAccessTime(ConcurrentDictionary<int, PointDataFX> dic)
        {
            try
            {
                if (dic == null || dic.Count == 0)
                    return;
                foreach (var item in dic.ToArray())
                {
                    if (DateTime.Now - item.Value.LastAccessTime > new TimeSpan(0, 0, 5))
                    {
                        dic.TryRemove(item.Key, out var temp);
                    }
                }
            }
            catch (Exception e)
            {
                //Log.Error("判断上次访问快照的时间时出错", e);
            }
        }

        /// <summary>
        /// 获取指定点的快照
        /// </summary>
        /// <param name="tagnames"></param>
        /// <returns></returns>
        public static List<PointData> GetSnapShot(string[] tagnames)
        {
            List<PointData> res = new List<PointData>();
            List<PointDataFX> not_in_snapshot = new List<PointDataFX>();
            Dictionary<int, string> dic_name_not_in_snapshot = new Dictionary<int, string>();
            foreach (var tag in tagnames)
            {
                if (TagPointManager.PointNameDictionary.TryGetValue(tag.ToLower(), out int id))
                {
                    if (TagPointManager.PointDictionary.TryGetValue(id, out PointDataFX point))
                    {
                        PointDataFX res_point = null;
                        bool GetSuccess = false;
                        switch (point.DataType)
                        {
                            case DataType.INT16:
                            case DataType.INT32:
                            case DataType.INT64:
                            case DataType.INT8:
                            case DataType.UINT16:
                            case DataType.UINT32:
                            case DataType.UINT8:
                            case DataType.BOOL:
                            case DataType.CHAR:
                                if (PointDictionaryInt.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryInt.TryAdd(id, point);
                                }
                                break;
                            case DataType.REAL16:
                            case DataType.REAL32:
                            case DataType.REAL64:
                                if (PointDictionaryFloat.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryFloat.TryAdd(id, point);
                                }
                                break;
                            case DataType.STRING:
                                if (PointDictionaryString.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryString.TryAdd(id, point);
                                }
                                break;
                            case DataType.BLOB:
                                if (PointDictionaryBlob.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryBlob.TryAdd(id, point);
                                }
                                break;
                            case DataType.COOR:
                                if (PointDictionaryCoor.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryCoor.TryAdd(id, point);
                                }
                                break;
                            case DataType.DateTime:
                                if (PointDictionaryDateTime.TryGetValue(id, out res_point))
                                {
                                    res_point.LastAccessTime = DateTime.Now;
                                    GetSuccess = true;
                                }
                                else
                                {
                                    PointDictionaryDateTime.TryAdd(id, point);
                                }
                                break;
                        }
                        if (GetSuccess)
                        {
                            PointData data = new PointData
                            {
                                Error = res_point.Error,
                                Value = res_point.Value,
                                Quality = res_point.Quality,
                                TagName = tag,
                                ErrorMsg = res_point.ErrorMsg,
                                ID = res_point.ID,
                                Time = res_point.DateTime.ToString("yyyy-MM-dd HH:mm:ss.fff")
                            };
                            res.Add(data);
                        }
                        else
                        {
                            //NEWDATA = true;
                            not_in_snapshot.Add(point);
                            dic_name_not_in_snapshot.Add(point.ID, tag);
                        }
                    }
                }
            }

            if (not_in_snapshot.Count > 0)
            {
                var int_input = (from item in not_in_snapshot where item.DataType == DataType.UINT8 || item.DataType == DataType.UINT16 || item.DataType == DataType.UINT32 || item.DataType == DataType.INT8 || item.DataType == DataType.INT16 || item.DataType == DataType.INT32 || item.DataType == DataType.INT64 || item.DataType == DataType.BOOL || item.DataType == DataType.CHAR select item.ID).ToList();
                var float_input = (from item in not_in_snapshot where item.DataType == DataType.REAL16 || item.DataType == DataType.REAL32 || item.DataType == DataType.REAL64 select item.ID).ToList();
                var string_input = (from item in not_in_snapshot where item.DataType == DataType.STRING select item.ID).ToList();
                var blob_input = (from item in not_in_snapshot where item.DataType == DataType.BLOB select item.ID).ToList();
                var coor_input = (from item in not_in_snapshot where item.DataType == DataType.COOR select item.ID).ToList();
                GetSnapShot(int_input, float_input, string_input, coor_input, blob_input,
    out var int_list, out var string_list, out var blob_list, out var coor_list, out var float_list);
                if (int_list != null)
                    foreach (var point in int_list.Data)
                    {
                        var data = new PointData
                        {
                            Error = point.Error,
                            Value = point.Value.ToString(),
                            Quality = point.Qualitie,
                            TagName = dic_name_not_in_snapshot[point.Id],
                            ErrorMsg = RTDBError.GetError(point.Error),
                            ID = point.Id,
                            Time = point.Time.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        res.Add(data);
                    }
                if (float_list != null)
                    foreach (var point in float_list.Data)
                    {
                        var data = new PointData
                        {
                            Error = point.Error,
                            Value = point.Value.ToString(),
                            Quality = point.Qualitie,
                            TagName = dic_name_not_in_snapshot[point.Id],
                            ErrorMsg = RTDBError.GetError(point.Error),
                            ID = point.Id,
                            Time = point.Time.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        res.Add(data);
                    }
                if (string_list != null)
                    foreach (var point in string_list.Data)
                    {
                        var data = new PointData
                        {
                            Error = point.Error,
                            Value = point.Value.ToString(),
                            Quality = point.Qualitie,
                            TagName = dic_name_not_in_snapshot[point.Id],
                            ErrorMsg = RTDBError.GetError(point.Error),
                            ID = point.Id,
                            Time = point.Time.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        res.Add(data);
                    }
                if (blob_list != null)
                    foreach (var point in blob_list.Data)
                    {
                        var data = new PointData
                        {
                            Error = point.Error,
                            Value = string.Join(string.Empty, point.Value.Select(tempByte => tempByte.ToString("D3"))),
                            Quality = point.Qualitie,
                            TagName = dic_name_not_in_snapshot[point.Id],
                            ErrorMsg = RTDBError.GetError(point.Error),
                            ID = point.Id,
                            Time = point.Time.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        res.Add(data);
                    }
                if (coor_list != null)
                    foreach (var point in coor_list.Data)
                    {
                        var data = new PointData
                        {
                            Error = point.Error,
                            Value = "X:" + point.X + ",Y:" + point.Y,
                            Quality = point.Qualitie,
                            TagName = dic_name_not_in_snapshot[point.Id],
                            ErrorMsg = RTDBError.GetError(point.Error),
                            ID = point.Id,
                            Time = point.Time.ToString("yyyy-MM-dd HH:mm:ss.fff")
                        };
                        res.Add(data);
                    }
            }

            return res;
        }

        /// <summary>
        /// 开启快照服务
        /// </summary>
        public static void Init()
        {
            //EXIT = false;
            PointDictionaryBlob = new ConcurrentDictionary<int, PointDataFX>();
            PointDictionaryCoor = new ConcurrentDictionary<int, PointDataFX>();
            PointDictionaryDateTime = new ConcurrentDictionary<int, PointDataFX>();
            PointDictionaryFloat = new ConcurrentDictionary<int, PointDataFX>();
            PointDictionaryInt = new ConcurrentDictionary<int, PointDataFX>();
            PointDictionaryString = new ConcurrentDictionary<int, PointDataFX>();
            //Polling();
            //TagRemovePolling();
            ThreadPool.QueueUserWorkItem(delegate { Polling(); });
            ThreadPool.QueueUserWorkItem(delegate { TagRemovePolling(); });
            //Log.Info("快照缓存池开启成功");
        }

        /// <summary>
        /// 关闭快照服务
        /// </summary>
        public static void Close()
        {
            //EXIT = true;
           // Log.Info("快照缓存池关闭成功");
        }

    }

}
