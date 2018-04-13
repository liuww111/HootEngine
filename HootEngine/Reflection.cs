using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Reflection;
using System.Collections;
using System.Text;
using Hoot.Common;
using System.Collections.Specialized;

namespace Hoot
{

    internal sealed class Reflection
    {
        // Sinlgeton pattern 4 from : http://csharpindepth.com/articles/general/singleton.aspx
        private static readonly Reflection instance = new Reflection();
        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit
        static Reflection()
        {
        }
        private Reflection()
        {
        }
        public static Reflection Instance { get { return instance; } }

        private SafeDictionary<Type, string> _tyname = new SafeDictionary<Type, string>();
        /// <summary>
        /// 获取程序集限定名
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        internal string GetTypeAssemblyName(Type t)
        {
            string val = "";
            if (_tyname.TryGetValue(t, out val))
                return val;
            else
            {
                string s = t.AssemblyQualifiedName;
                _tyname.Add(t, s);
                return s;
            }
        }
        
        internal void ClearReflectionCache()
        {
            _tyname = new SafeDictionary<Type, string>();
        }
    }
}
