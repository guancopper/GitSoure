using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace WebAPICore.Struct
{
    /// <summary>
    /// 历史数据实体
    /// </summary>
    [DataContract]
    public class DataEntity
    {

        ///// <summary>
        ///// 测点id
        ///// </summary>
        //public int Id;

        /// <summary>
        /// 时间戳
        /// </summary>
        [DataMember]
        public string Time
        {
            get
            {
                if (string.IsNullOrEmpty(FormatString))
                {
                    return DateTime + "." + Ms;
                }
                else
                {
                    return DateTime.ToString(FormatString);
                }
            }
        }

        public string FormatString = string.Empty;

        /// <summary>
        /// 时间戳
        /// </summary>
        public DateTime DateTime;

        /// <summary>
        /// 时间戳
        /// </summary>
        [DataMember]
        public short Ms;

        /// <summary>
        /// 值
        /// </summary>
        [DataMember]
        public string Value;

        /// <summary>
        /// 数量
        /// 获取功率曲线数据（平均风速，平均功率，频次），报表用。
        /// </summary>
        [DataMember]
        public int Count { get; set; } = 0;

    }
}
