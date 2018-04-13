using Hoot;
using System;
using System.IO;

namespace CoreConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("当前工作目录："+ Environment.CurrentDirectory);
            HootEngine hootEngine = new HootEngine(Path.Combine(Environment.CurrentDirectory,"hoot"), "hoot", true);
            Console.WriteLine("Hello Hoot!");
            
            string[] files = Directory.GetFiles(Path.Combine(Environment.CurrentDirectory, "textLibrary"), "*", SearchOption.AllDirectories);

            Console.WriteLine($"files:{files.Length}"); // 新语法
            int i = 0;
            foreach (string fn in files)
            {
                try
                {
                    if (hootEngine.IsIndexed(fn) == false)
                    {
                        Console.WriteLine($"file path:{fn}"); // 新语法
                        TextReader tr = new StreamReader(fn);
                        string s = "";
                        if (tr != null)
                            s = tr.ReadToEnd();
                        hootEngine.Index(new myDoc(new FileInfo(fn), s), true);
                    }
                }
                catch(Exception ex){
                    Console.WriteLine(ex);
                }
                i++;
                if (i > 1000)
                {
                    i = 0;
                    hootEngine.Save();
                }
            }
            hootEngine.Save();
            hootEngine.OptimizeIndex();

            while (true) {
                Console.WriteLine("input key!");
                string inputTxt = Console.ReadLine();
                Console.WriteLine("input:" + inputTxt);
                foreach (var d in hootEngine.FindDocumentFileNames(inputTxt))
                {
                    Console.WriteLine(d);
                }
            }
            //Console.ReadKey();
        }
        
    }
}
