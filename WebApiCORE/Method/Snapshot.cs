using System;
using System.Collections.Generic;
using System.Text;
using WebAPICore.Struct;
using GoldenAPI.Impl;
using GoldenAPI.Model.Data;
using GoldenAPI.Model.Base;
using GoldenAPI.Common;
using System.Linq;
using GoldenAPI.Model;
using GoldenAPI;
using WebApICore.Base;

namespace WebAPICore.Method
{
    public static class Snapshot
    {

        public static EntityResult PutSnapshot(List<PointData> points)
        {
            List<PointData> nonExistList = new List<PointData>();
            var pointDataMap = GetPointDataMap(points, out nonExistList);
            EntityResult er = new EntityResult();

            var conn = ConnectionPool.TryGetConnection();
            try
            {
                var snapShot = new SnapshotImpl(conn);
                var result = new EntityResult { ErrorList = new List<PointData>() };
                foreach (var item in pointDataMap)
                {
                    switch (item.Key)
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
                            var tempResult = PutIntSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                        case DataType.REAL16:
                        case DataType.REAL32:
                        case DataType.REAL64:
                            tempResult = PutFloatSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                        case DataType.STRING:
                            tempResult = PutStringSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                        case DataType.BLOB:
                            tempResult = PutBlobSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                        case DataType.COOR:
                            tempResult = PutCoorSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                        case DataType.DateTime:
                            tempResult = PutDateTimeSnapshots(item.Value, snapShot);
                            result = CombineEntityResult(result, tempResult);
                            break;
                    }
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

        #region 写入快照

        private static Dictionary<DataType, Dictionary<int, PointData>> GetPointDataMap(IEnumerable<PointData> pointDataCollection, out List<PointData> nonExistentList)
        {
            var pointDataMap = new Dictionary<DataType, Dictionary<int, PointData>>(5);
            List<PointData> pList = new List<PointData>();
            foreach (var item in pointDataCollection)
            {
                PointDataFX pointInfo = null;
                if (TagPointManager.PointNameDictionary.ContainsKey(item.TagName.ToLower()))
                {
                    var id = TagPointManager.PointNameDictionary[item.TagName.ToLower()];
                    if(TagPointManager.PointDictionary.ContainsKey(id))
                    {
                        pointInfo = TagPointManager.PointDictionary[id];
                    }
                }
                if (pointInfo == null)
                {
                    PointData ptdata = new PointData();
                    ptdata.ID = 0;
                    ptdata.TagName = item.TagName;
                    ptdata.Time = DateTime.Now.ToString("yyyy-MM-dd");
                    ptdata.Value = "";
                    ptdata.Error = 0;
                    ptdata.ErrorMsg = "错误码为:0；错误信息为:指定的标签点不存在！";
                    pList.Add(ptdata);

                }
                else
                {
                    if (pointDataMap.ContainsKey(pointInfo.DataType))
                    {
                        var idPointDataCollection = pointDataMap[pointInfo.DataType];
                        idPointDataCollection.Add(pointInfo.ID, item);
                    }
                    else
                    {
                        var idPointDataCollection = new Dictionary<int, PointData> { { pointInfo.ID, item } };
                        pointDataMap[pointInfo.DataType] = idPointDataCollection;
                    }
                }
            }
            nonExistentList = pList;
            return pointDataMap;
        }

        /// <summary>
        /// 插入数据(修改版)
        /// </summary>
        /// <param name="pointDataMap">映射</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns></returns>
        private static EntityResult PutData(Dictionary<DataType, Dictionary<int, PointData>> pointDataMap, SnapshotImpl snapShot, List<PointData> nonExistList)
        {
            var result = new EntityResult { ErrorList = new List<PointData>() };
            foreach (var item in pointDataMap)
            {
                switch (item.Key)
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
                        var tempResult = PutIntSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                    case DataType.REAL16:
                    case DataType.REAL32:
                    case DataType.REAL64:
                        tempResult = PutFloatSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                    case DataType.STRING:
                        tempResult = PutStringSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                    case DataType.BLOB:
                        tempResult = PutBlobSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                    case DataType.COOR:
                        tempResult = PutCoorSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                    case DataType.DateTime:
                        tempResult = PutDateTimeSnapshots(item.Value, snapShot);
                        result = CombineEntityResult(result, tempResult);
                        break;
                }
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

        /// <summary>
        /// 插入Int类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutIntSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var entity = new Entity<IntData>
            {
                Data = (from item in dictionary
                        select new IntData
                        {
                            Id = item.Key,
                            Ms = (short)DateTime.Now.Millisecond,
                            Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                            Time = DateTime.Now,
                            Value = Convert.ToInt64(item.Value.Value)
                        }).ToList()
            };
            var count = snapShot.PutIntSnapshots(entity);
            var result = new EntityResult { SuccessCount = count };
            if (count >= entity.Data.Count)
            { return result; }

            result.ErrorList = new List<PointData>(entity.Data.Count - count);
            foreach (var item in entity.Data)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 插入Float类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutFloatSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var entity = new Entity<FloatData>
            {
                Data = (from item in dictionary
                        select new FloatData
                        {
                            Id = item.Key,
                            Ms = (short)DateTime.Now.Millisecond,
                            Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                            Time = DateTime.Now,
                            Value = Convert.ToDouble(item.Value.Value)
                        }).ToList()
            };
            //foreach (var item_ in entity.Data) {
            //    if (item_.Time == Convert.ToDateTime("1900-01-01 00:00:00")) 
            //    {
            //        item_.Time = new DateTime(0);
            //    }
            //}

            ////以下为时间戳置零修改部分

            //List<FloatData> list = new List<FloatData>();
            //for (int  i=0;i<entity.Data.Count;i++) {

            //    list[i].Id = entity.Data[i].Id;
            //    list[i].Ms = entity.Data[i].Ms;
            //    list[i].Time = entity.Data[i].Time;
            //    list[i].Qualitie = entity.Data[i].Qualitie;
            //    list[i].Value = entity.Data[i].Value;
            //}
            //EntityResult<FloatData> result_ = snapShot.PutFloatSnapshotsCollection(list);
            //var count = result_.SuccessCount;
            ////以上为时间戳置零修改部分


            var count = snapShot.PutFloatSnapshots(entity);
            var result = new EntityResult { SuccessCount = count };
            if (count >= entity.Data.Count)
            { return result; }

            result.ErrorList = new List<PointData>(entity.Data.Count - count);
            foreach (var item in entity.Data)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 插入string类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutStringSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var entity = new Entity<StringData>
            {
                Data = (from item in dictionary
                        select new StringData
                        {
                            Id = item.Key,
                            Ms = (short)DateTime.Now.Millisecond,
                            Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                            Time = DateTime.Now,
                            Value = item.Value.Value
                        }).ToList()
            };
            var count = snapShot.PutStringSnapshots(entity);
            var result = new EntityResult { SuccessCount = count };
            if (count >= entity.Data.Count)
            { return result; }

            result.ErrorList = new List<PointData>(entity.Data.Count - count);
            foreach (var item in entity.Data)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 插入Blob类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutBlobSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var entity = new Entity<BlobData>
            {
                Data = (from item in dictionary
                        select new BlobData
                        {
                            Id = item.Key,
                            Ms = (short)DateTime.Now.Millisecond,
                            Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                            Time = DateTime.Now,
                            Value = Encoding.UTF8.GetBytes(item.Value.Value)
                        }).ToList()
            };
            var count = snapShot.PutBlobSnapshots(entity);
            var result = new EntityResult { SuccessCount = count };
            if (count >= entity.Data.Count)
            { return result; }

            result.ErrorList = new List<PointData>(entity.Data.Count - count);
            foreach (var item in entity.Data)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 插入Coor类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutCoorSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var entity = new Entity<CoorData>
            {
                Data = (from item in dictionary
                        select new CoorData
                        {
                            Id = item.Key,
                            Ms = (short)DateTime.Now.Millisecond,
                            Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                            Time = DateTime.Now,
                            X = GetCoorValue(item.Value.Value, "X"),
                            Y = GetCoorValue(item.Value.Value, "Y")
                        }).ToList()
            };
            var count = snapShot.PutCoorSnapshots(entity);
            var result = new EntityResult { SuccessCount = count };
            if (count >= entity.Data.Count)
            { return result; }

            result.ErrorList = new List<PointData>(entity.Data.Count - count);
            foreach (var item in entity.Data)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 获取Coor的X和Y的值
        /// </summary>
        /// <param name="value"></param>
        /// <param name="text"></param>
        /// <returns></returns>
        private static float GetCoorValue(string value, string text)
        {
            var array = value.Split(',');
            return float.Parse(text.Equals("X") ? array[0].Replace("X:", string.Empty) : array[1].Replace("Y:", string.Empty));
        }

        /// <summary>
        /// 插入DateTime类型快照
        /// </summary>
        /// <param name="dictionary">ID和PointData组成的字典</param>
        /// <param name="snapShot">快照接口实现类</param>
        /// <returns>包装有成功数量和失败集合的类</returns>
        private static EntityResult PutDateTimeSnapshots(Dictionary<int, PointData> dictionary, SnapshotImpl snapShot)
        {
            var dateTimeDataCollection = (from item in dictionary
                                          let dt = Convert.ToDateTime(item.Value.Time)
                                          select new DateTimeData
                                          {
                                              Id = item.Key,
                                              Qualitie = (Quality)Convert.ToInt32(item.Value.Quality),
                                              Time = DateTime.Now,
                                              Value = Convert.ToDateTime(item.Value.Value)
                                          }).ToList();
            List<DateTimeData> errorList;
            var count = snapShot.PutDateTimeSnapshots(dateTimeDataCollection, out errorList);
            var result = new EntityResult { SuccessCount = count };
            if (errorList == null || errorList.Count == 0)
            { return result; }

            result.ErrorList = new List<PointData>();
            foreach (var item in errorList)
            {
                if (item.Error <= 0)
                { continue; }

                var temp = dictionary[item.Id];
                temp.Error = item.Error;
                temp.ErrorMsg = RTDBError.GetError(temp.Error);
                result.ErrorList.Add(temp);
            }
            return result;
        }

        /// <summary>
        /// 将两个EntityResult中的信息合并
        /// </summary>
        /// <param name="first">第一个EntityResult</param>
        /// <param name="second">第二个EntityResult</param>
        /// <returns>合并后的EntityResult</returns>
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
