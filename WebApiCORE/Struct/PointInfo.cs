using System;
using System.Collections.Generic;
using System.Text;
using GoldenAPI.Model.Base;

namespace WebAPICore.Struct
{
    /// <summary>
    /// 标签点
    /// </summary>
    public class PointInfo
    {
        /// <summary>
        /// 标签点id
        /// </summary>
        public int ID { get; set; }

        /// <summary>
        /// 标签点类型
        /// </summary>
        public DataType Type { get; set; }
    }
}
