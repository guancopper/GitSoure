using System;
using System.Collections.Generic;
using System.Text;

namespace WebAPICore.Struct
{
    /// <summary>
    /// 写入返回实体
    /// </summary>
    public class EntityResult
    {
        /// <summary>
        /// 成功的条数
        /// </summary>
        public int SuccessCount;

        /// <summary>
        /// 失败的数据
        /// </summary>
        public List<PointData> ErrorList = new List<PointData>();

    }
}
