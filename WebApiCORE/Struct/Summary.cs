using System;
using System.Collections.Generic;
using System.Text;

namespace WebAPICore.Struct
{
    /// <summary>
    /// 统计实体
    /// </summary>
    public class Summary
    {
        /// <summary>
        /// 最大值
        /// </summary>
        public double Max;

        /// <summary>
        /// 最小值
        /// </summary>
        public double Min;

        /// <summary>
        /// 表示统计时间段内的累计值，结果的单位为标签点的工程单位(计算的是面积)
        /// </summary>
        public double Total;

        /// <summary>
        /// 表示统计时间段内的算术累积值
        /// </summary>
        public double CalcTotal;

        /// <summary>
        /// 表示统计时间段内的算术平均值
        /// </summary>
        public double CalcAvg;

        /// <summary>
        /// 表示统计时间段内的加权平均值
        /// </summary>
        public double PowerAvg;
        /// <summary>
        /// 表示统计时间段内的差值
        /// </summary>
        public double Difference;

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime;

        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime;

        /// <summary>
        /// 同比去年总量  百分比
        /// </summary>
        public double EqLastYear;
        /// <summary>
        /// 可利用率
        /// </summary>
        public double Availble;
    }
}
