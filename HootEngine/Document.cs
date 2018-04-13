using System;
using System.Xml.Serialization;
using System.IO;
using Newtonsoft.Json;

namespace Hoot
{
    public class Document
    {
        public Document()
        {
            DocNumber = -1;
        }
        public Document(FileInfo fileinfo, string text)
        {
            FileName = fileinfo.FullName;
            ModifiedDate = fileinfo.LastWriteTime;
            FileSize = fileinfo.Length;
            Text = text;
            DocNumber = -1;
        }
        public int DocNumber { get; set; }
        /// <summary>
        /// 内容
        /// </summary>
        [XmlIgnore]
        [JsonIgnore]
        public string Text { get; set; }
        /// <summary>
        /// 完整路径名
        /// </summary>
        public string FileName { get; set; }
        /// <summary>
        /// 最后一次写入的时间
        /// </summary>
        public DateTime ModifiedDate { get; set; }
        /// <summary>
        /// 文件大小
        /// </summary>
        public long FileSize;

        public override string ToString()
        {
            return FileName;
        }
    }

    internal enum OPERATION
    {
        AND,
        OR,
        ANDNOT
    }
}
