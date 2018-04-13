using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace Hoot
{
    //运行日志
    public interface ILog
    {
        /// <summary>
        /// Fatal log = log level 5
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="objs"></param>
        void Fatal(object msg, params object[] objs); // 5
        /// <summary>
        /// Error log = log level 4
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="objs"></param>
        void Error(object msg, params object[] objs); // 4
        /// <summary>
        /// Warning log = log level 3
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="objs"></param>
        void Warn(object msg, params object[] objs);  // 3
        /// <summary>
        /// Debug log = log level 2 
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="objs"></param>
        void Debug(object msg, params object[] objs); // 2
        /// <summary>
        /// Info log = log level 1
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="objs"></param>
        void Info(object msg, params object[] objs);  // 1
    }

    internal class Logger : ILog
    {
        public Logger(Type type)
        {
            typename = type.Namespace + "." + type.Name;
        }

        private string typename = "";

        private void Log(string logtype, string msg, params object[] objs)
        {
            System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(2);
            System.Diagnostics.StackFrame sf = st.GetFrame(0);
            string meth = sf.GetMethod().Name;
            this.Log(logtype, typename, meth, msg, objs);
        }

        private string FormatLog(string log, string type, string meth, string msg, object[] objs)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            sb.Append("|");
            sb.Append(log);
            sb.Append("|");
            sb.Append(Thread.CurrentThread.ManagedThreadId.ToString());
            sb.Append("|");
            sb.Append(type);
            sb.Append("|");
            sb.Append(meth);
            sb.Append("| ");
            sb.AppendLine(msg);

            if (objs != null)
                foreach (object o in objs)
                    sb.AppendLine("" + o);

            return sb.ToString();
        }

        public void Log(string logtype, string type, string meth, string msg, params object[] objs)
        {
            var l = FormatLog(logtype, type, meth, msg, objs);
            System.Diagnostics.Debug.WriteLine(l);
            //System.Console.WriteLine(l);
        }

        public void Fatal(object msg, params object[] objs)
        {
            Log("FATAL", "" + msg, objs);
        }

        public void Error(object msg, params object[] objs)
        {
            Log("ERROR", "" + msg, objs);
        }

        public void Warn(object msg, params object[] objs)
        {
            Log("WARN", "" + msg, objs);
        }

        public void Debug(object msg, params object[] objs)
        {
            Log("DEBUG", "" + msg, objs);
        }

        public void Info(object msg, params object[] objs)
        {
            Log("INFO", "" + msg, objs);
        }
    }
    /// <summary>
    /// Log记录库
    /// </summary>
    public static class LogManager
    {
        public static ILog GetLogger(Type obj)
        {
            return new Logger(obj);
        }
    }
}
