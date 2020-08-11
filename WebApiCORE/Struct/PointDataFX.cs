using System;
using System.Collections.Generic;
using System.Text;
using GoldenAPI.Model;
using GoldenAPI.Model.Base;

namespace WebAPICore.Struct
{
    /// <summary>
    /// PointDataFX
    /// </summary>
    public class PointDataFX : PointData
    {
        /// <summary>
        /// 快照时间
        /// </summary>
        public DateTime DateTime = DateTime.Now;

        /// <summary>
        /// 访问时间
        /// </summary>
        public DateTime LastAccessTime = DateTime.Now;

        /// <summary>
        /// 标签点类型
        /// </summary>
        public DataType DataType = DataType.INT32;

        /// <summary>
        /// 构造函数
        /// </summary>
        public PointDataFX()
            : base()
        {

        }

    }

    /// <summary>
    /// 写入数据实体
    /// </summary>
    [Serializable]
    public class PointData
    {
        /// <summary>
        /// 测点ID
        /// </summary>
        public int ID;

        /// <summary>
        /// 测点全名称
        /// </summary>
        public string TagName;

        /// <summary>
        /// 时间戳
        /// </summary>
        public string Time;

        /// <summary>
        /// 值
        /// </summary>
        public string Value;

        ///// <summary>
        ///// 质量码
        ///// </summary>
        public Quality Quality;

        /// <summary>
        /// 小错误码
        /// </summary>
        public uint Error;

        /// <summary>
        /// 错误码描述
        /// </summary>
        public string ErrorMsg;
    }


}
