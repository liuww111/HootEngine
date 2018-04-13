using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoot
{
    public interface ITokenizer
    {
        /// <summary>
        /// 分词，得到词的字典，和词对应的频率
        /// </summary>
        /// <param name="txt"></param>
        /// <returns></returns>
        Dictionary<string, int> GenerateWordFreq(string txt);
    }
}
