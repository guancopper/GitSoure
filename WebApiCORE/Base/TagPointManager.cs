using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using WebAPICore.Struct;
using GoldenAPI;
using GoldenAPI.Impl;
using GoldenAPI.Model.Base;

namespace WebApICore.Base
{
    public static class TagPointManager
    {
        /// <summary>
        /// 点ID与点映射关系
        /// </summary>
        public static ConcurrentDictionary<int, PointDataFX> PointDictionary { get; set; } =
            new ConcurrentDictionary<int, PointDataFX>();

        /// <summary>
        /// 点名与点ID映射关系
        /// </summary>
        public static ConcurrentDictionary<string, int> PointNameDictionary { get; set; } =
            new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        /// <summary>
        /// 初始化完成
        /// </summary>
        public static bool InitDone { get; set; } = false;

        /// <summary>
        /// 标签点更新线程
        /// </summary>
        private static void TagUpdatePolling() 
        {
            Stopwatch watch = new Stopwatch();
            watch.Restart();
            while (true) 
            {
                if (watch.Elapsed > new TimeSpan(0, 0, 60) || !InitDone)
                {
                    watch.Restart();
                    try 
                    {
                        List<FullPoint> list = new List<FullPoint>();
                        var Connection = ConnectionPool.TryGetConnection();
                        BaseImpl ibase = new BaseImpl(Connection);
                        int total = ibase.SearchPointsCount(new SearchConditionTotal());
                        var id_list = ibase.Search(new SearchCondition(), total, DataSort.ID);
                        list = ibase.GetPointsProperty(id_list);
                        if (Connection != null)
                        {
                            Connection.Close();
                        }
                        if (list.Count > 0)
                        {
                            TagUpdatePollingTask(list);
                        }
                    }
                    catch (Exception ex) 
                    {
                        Console.WriteLine("标签点轮询管理异常" + ex.ToString());
                    }
                    GC.Collect();
                }
                //休眠1s
                Thread.Sleep(1000);
            }
        }

        /// <summary>
        /// 标签点更新
        /// </summary>
        private static void TagUpdatePollingTask(List<FullPoint> list)
        {
            foreach (var point in list)
            {
                if (string.IsNullOrEmpty(point.BasePoint.TableDotTag))
                {
                    Console.WriteLine("标签点轮询时 ID 为 {0} 的点异常", point.BasePoint.Id);
                    //Log.Debug(string.Format("标签点轮询时 ID 为 {0} 的点异常", point.BasePoint.Id));
                    continue;
                }
                if (PointDictionary.ContainsKey(point.BasePoint.Id))
                {
                    var old_name = PointDictionary[point.BasePoint.Id].TagName;
                    var new_name = point.BasePoint.TableDotTag.ToLower();
                    PointDictionary[point.BasePoint.Id].TagName = new_name;
                    if (string.IsNullOrEmpty(old_name))
                        old_name = "";
                    if (PointNameDictionary.ContainsKey(old_name))
                    {
                        if (new_name != old_name)
                        {
                            PointNameDictionary.TryRemove(old_name, out int value);
                            PointNameDictionary.TryAdd(new_name, point.BasePoint.Id);
                            
                        }
                        else
                        {
                            PointNameDictionary[old_name] = point.BasePoint.Id;
                        }
                    }
                    else
                    {
                        PointNameDictionary.TryAdd(new_name, point.BasePoint.Id);
                    }
                }
                else
                {
                    var new_point = new PointDataFX
                    {
                        ID = point.BasePoint.Id,
                        TagName = point.BasePoint.TableDotTag.ToLower(),
                        DataType = point.BasePoint.DataType
                    };
                    PointNameDictionary.TryAdd(point.BasePoint.TableDotTag.ToLower(), point.BasePoint.Id);
                    PointDictionary.TryAdd(point.BasePoint.Id, new_point);
                }
            }
        }

        /// <summary>
        /// 初始化
        /// </summary>
        public static void Init()
        {
            PointDictionary = new ConcurrentDictionary<int, PointDataFX>();
            PointNameDictionary = new ConcurrentDictionary<string, int>();
            ThreadPool.QueueUserWorkItem(delegate { TagUpdatePolling(); });
            //Log.Info("标签点缓存池开启成功");
            Console.WriteLine("标签点缓存池开启成功");
        }
        /// <summary>
        /// 根据点名获取点
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public static Dictionary<string, PointInfo> GetPointInfosByTagNames(string[] tags)
        {
            Dictionary<string, PointInfo> points = new Dictionary<string, PointInfo>();
            foreach (var tag in tags)
            {
                if (PointNameDictionary.ContainsKey(tag.ToLower()))
                {
                    var id = PointNameDictionary[tag.ToLower()];
                    if (PointDictionary.ContainsKey(id))
                    {
                        var point = PointDictionary[id];
                        points.Add(tag, new PointInfo()
                        {
                            ID = point.ID,
                            Type = point.DataType
                        });
                    }
                }
            }
            return points;
        }
    }
}
