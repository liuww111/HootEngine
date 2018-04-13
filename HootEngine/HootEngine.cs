using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using Hoot.Common;
using Newtonsoft.Json;

namespace Hoot
{
    public class HootEngine
    {
        /// <summary>
        /// 词字典
        /// </summary>
        private SafeDictionary<string, int> _words = new SafeDictionary<string, int>();
        private BitmapIndex _bitmaps;
        private BoolIndex _deleted;
        /// <summary>
        /// 日志
        /// </summary>
        private ILog _log = LogManager.GetLogger(typeof(HootEngine));
        /// <summary>
        /// json
        /// </summary>
        private IJson _json = JsonManager.GetJsonNet();
        /// <summary>
        /// 文档最后编号
        /// </summary>
        private int _lastDocNum = 0;
        private string _FileName = "words";
        /// <summary>
        /// 存储地址
        /// </summary>
        private string _Path = "";
        private KeyStoreString _docs;
        /// <summary>
        /// 是否锁文档模式（文档模式会存储文档）
        /// </summary>
        private bool _docMode = false;
        /// <summary>
        /// 词发生改变
        /// </summary>
        private bool _wordschanged = false;
        /// <summary>
        /// 锁对象
        /// </summary>
        private object _lock = new object();

        private ITokenizer _tokenizer=new DefaultTokenizer();

        /// <summary>
        /// 创建hoot引擎
        /// </summary>
        /// <param name="IndexPath">索引地址目录，全路径</param>
        /// <param name="FileName">文件名（最好无后缀）</param>
        /// <param name="DocMode">是否文档模式</param>
        public HootEngine(string IndexPath, string FileName, bool DocMode)
        {
            _Path = IndexPath;
            _FileName = FileName;
            _docMode = DocMode;

            if (_Path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false) _Path += Path.DirectorySeparatorChar;
            if (Directory.Exists(IndexPath)) { Directory.CreateDirectory(IndexPath); }
            
            _log.Debug("hOOOOOOt Version:" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString());
            _log.Debug("Starting hOOOOOOt....");
            _log.Debug("Storage Folder = " + _Path);

            if (DocMode)
            {
                _docs = new KeyStoreString(_Path, FileName, false);
                // read deleted
                _deleted = new BoolIndex(_Path, FileName);
                _lastDocNum = (int)_docs.Count();//得到文档数，用于编号
            }
            _bitmaps = new BitmapIndex(_Path, _FileName);
            // read words
            LoadWords();
        }
        /// <summary>
        /// 设置分词器
        /// </summary>
        /// <param name="tokenizer"></param>
        public void SetTokenizer(ITokenizer tokenizer) {
            _tokenizer = tokenizer;
        }

        public string[] Words
        {
            get { checkloaded(); return _words.Keys(); }
        }

        public int WordCount
        {
            get { checkloaded(); return _words.Count; }
        }

        public int DocumentCount
        {
            get { checkloaded(); return _lastDocNum - (int)_deleted.GetBits().CountOnes(); }
        }

        public string IndexPath { get { return _Path; } }

        public void Save()
        {
            lock (_lock)
                InternalSave();
        }

        public void Index(int recordnumber, string text)
        {
            checkloaded();
            AddtoIndex(recordnumber, text);
        }

        public WAHBitArray Query(string filter, int maxsize)
        {
            checkloaded();
            return ExecutionPlan(filter, maxsize);
        }

        public int Index(Document doc, bool deleteold)
        {
            checkloaded();
            _log.Info("indexing doc : " + doc.FileName);
            DateTime dt = FastDateTime.Now;

            if (deleteold && doc.DocNumber > -1)
                _deleted.Set(true, doc.DocNumber);

            if (deleteold == true || doc.DocNumber == -1)
                doc.DocNumber = _lastDocNum++;//新增编号

            // save doc to disk
            //string dstr = fastJSON.JSON.ToJSON(doc, new fastJSON.JSONParameters { UseExtensions = false });//fastJSON
            string dstr = _json.SerializeObject(doc);
            //存储doc模型对象
            _docs.Set(doc.FileName.ToLower(), Encoding.Unicode.GetBytes(dstr));

            _log.Info("writing doc to disk (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            dt = FastDateTime.Now;
            // index doc
            AddtoIndex(doc.DocNumber, doc.Text);
            _log.Info("indexing time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            return _lastDocNum;
        }

        public IEnumerable<int> FindRows(string filter)
        {
            checkloaded();
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate records
            return bits.GetBitIndexes();
        }

        public IEnumerable<T> FindDocuments<T>(string filter)
        {
            checkloaded();
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (int i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                string b = _docs.ReadData(i);
                //T d = fastJSON.JSON.ToObject<T>(b, new fastJSON.JSONParameters { ParametricConstructorOverride = true }); //fastJSON
                T d = _json.DeserializeObject<T>(b);

                yield return d;
            }
        }

        public IEnumerable<string> FindDocumentFileNames(string filter)
        {
            checkloaded();
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (int i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                string b = _docs.ReadData(i);
                //var d = (Dictionary<string, object>)fastJSON.JSON.Parse(b);//fastJSON
                //yield return d["FileName"].ToString();
                var d = _json.DeserializeObject<Document>(b);
                yield return d.FileName;
            }
        }

        public void RemoveDocument(int number)
        {
            // add number to deleted bitmap
            _deleted.Set(true, number);
        }

        public bool RemoveDocument(string filename)
        {
            // remove doc based on filename
            byte[] b;
            if (_docs.Get(filename.ToLower(), out b))
            {
                //Document d = fastJSON.JSON.ToObject<Document>(Encoding.Unicode.GetString(b));//fastJSON
                Document d = _json.DeserializeObject<Document>(Encoding.Unicode.GetString(b));
                RemoveDocument(d.DocNumber);
                return true;
            }
            return false;
        }

        public bool IsIndexed(string filename)
        {
            byte[] b;
            return _docs.Get(filename.ToLower(), out b);
        }

        public void OptimizeIndex()
        {
            lock (_lock)
            {
                InternalSave();
                _bitmaps.Optimize();
            }
        }

        public void Shutdown()
        {
            lock (_lock)
            {
                InternalSave();
                if (_deleted != null)
                {
                    _deleted.SaveIndex();
                    _deleted.Shutdown();
                    _deleted = null;
                }

                if (_bitmaps != null)
                {
                    _bitmaps.Commit(Global.FreeBitmapMemoryOnSave);
                    _bitmaps.Shutdown();
                    _bitmaps = null;
                }

                if (_docMode)
                    _docs.Shutdown();
            }
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                InternalSave();

                if (_deleted != null)
                    _deleted.FreeMemory();

                if (_bitmaps != null)
                    _bitmaps.FreeMemory();

                if (_docs != null)
                    _docs.FreeMemory();
            }
        }

        internal T Fetch<T>(int docnum)
        {
            string b = _docs.ReadData(docnum);
            //return fastJSON.JSON.ToObject<T>(b);//fastJSON
            return _json.DeserializeObject<T>(b);
        }
        /// <summary>
        /// 检查载入
        /// </summary>
        private void checkloaded()
        {
            if (_wordschanged == false)
            {
                LoadWords();
            }
        }

        private WAHBitArray ExecutionPlan(string filter, int maxsize)
        {
            //_log.Debug("query : " + filter);
            DateTime dt = FastDateTime.Now;
            // query indexes
            string[] words = filter.Split(' ');
            //bool defaulttoand = true;
            //if (filter.IndexOfAny(new char[] { '+', '-' }, 0) > 0)
            //    defaulttoand = false;

            WAHBitArray found = null;// WAHBitArray.Fill(maxsize);            

            foreach (string s in words)
            {
                int c;
                bool not = false;
                string word = s;
                if (s == "") continue;

                OPERATION op = OPERATION.AND;
                //if (defaulttoand)
                //    op = OPERATION.AND;

                if (word.StartsWith("+"))
                {
                    op = OPERATION.OR;
                    word = s.Replace("+", "");
                }

                if (word.StartsWith("-"))
                {
                    op = OPERATION.ANDNOT;
                    word = s.Replace("-", "");
                    not = true;
                    if (found == null) // leading with - -> "-oak hill"
                    {
                        found = WAHBitArray.Fill(maxsize);
                    }
                }

                if (word.Contains("*") || word.Contains("?"))
                {
                    WAHBitArray wildbits = new WAHBitArray();

                    // do wildcard search
                    Regex reg = new Regex("^" + word.Replace("*", ".*").Replace("?", ".") + "$", RegexOptions.IgnoreCase);
                    foreach (string key in _words.Keys())
                    {
                        if (reg.IsMatch(key))
                        {
                            _words.TryGetValue(key, out c);
                            WAHBitArray ba = _bitmaps.GetBitmap(c);

                            wildbits = DoBitOperation(wildbits, ba, OPERATION.OR, maxsize);
                        }
                    }
                    if (found == null)
                        found = wildbits;
                    else
                    {
                        if (not) // "-oak -*l"
                            found = found.AndNot(wildbits);
                        else if (op == OPERATION.AND)
                            found = found.And(wildbits);
                        else
                            found = found.Or(wildbits);
                    }
                }
                else if (_words.TryGetValue(word.ToLowerInvariant(), out c))
                {
                    // bits logic
                    WAHBitArray ba = _bitmaps.GetBitmap(c);
                    found = DoBitOperation(found, ba, op, maxsize);
                }
                else if (op == OPERATION.AND)
                    found = new WAHBitArray();
            }
            if (found == null)
                return new WAHBitArray();

            // remove deleted docs
            WAHBitArray ret;
            if (_docMode)
                ret = found.AndNot(_deleted.GetBits());
            else
                ret = found;
            //_log.Debug("query time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return ret;
        }

        private static WAHBitArray DoBitOperation(WAHBitArray bits, WAHBitArray c, OPERATION op, int maxsize)
        {
            if (bits != null)
            {
                switch (op)
                {
                    case OPERATION.AND:
                        bits = bits.And(c);
                        break;
                    case OPERATION.OR:
                        bits = bits.Or(c);
                        break;
                    case OPERATION.ANDNOT:
                        bits = bits.And(c.Not(maxsize));
                        break;
                }
            }
            else
                bits = c;
            return bits;
        }

        private void InternalSave()
        {
            _log.Debug("saving index...");
            DateTime dt = FastDateTime.Now;
            // save deleted
            if (_deleted != null)
                _deleted.SaveIndex();

            // save docs 
            if (_docMode)
                _docs.SaveIndex();

            if (_bitmaps != null)
                _bitmaps.Commit(false);

            if (_words != null && _wordschanged == true)
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8);

                // save words and bitmaps
                using (FileStream words = new FileStream(Path.Combine(_Path , _FileName + Global.WordsExt), FileMode.Create))
                {
                    var keys = _words.Keys();
                    int c = keys.Length;
                    _log.Debug("key count = " + c);
                    foreach (string key in _words.Keys())
                    {
                        try//FIX : remove when bug found
                        {
                            bw.Write(key);
                            bw.Write(_words[key]);
                        }
                        catch (Exception ex)
                        {
                            _log.Error(" on key = " + key);
                            throw ex;
                        }
                    }
                    byte[] b = ms.ToArray();
                    words.Write(b, 0, b.Length);
                    words.Flush();
                    words.Close();
                }
            }
            _log.Debug("save time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
        }
        /// <summary>
        /// 载入词字典
        /// </summary>
        private void LoadWords()
        {
            lock (_lock)
            {
                if (_words == null)
                    _words = new SafeDictionary<string, int>();
                if (File.Exists(Path.Combine(_Path , _FileName + Global.WordsExt)) == false)
                    return;
                // load words
                byte[] b = File.ReadAllBytes(Path.Combine(_Path , _FileName + Global.WordsExt));
                if (b.Length == 0)
                    return;
                MemoryStream ms = new MemoryStream(b);
                BinaryReader br = new BinaryReader(ms, Encoding.UTF8);
                string s = br.ReadString();
                while (s != "")
                {
                    int off = br.ReadInt32();
                    _words.Add(s, off);
                    try
                    {
                        s = br.ReadString();
                    }
                    catch { s = ""; }
                }
                _log.Debug("Word Count = " + _words.Count);
                _wordschanged = true;
            }
        }
        /// <summary>
        /// 添加索引
        /// </summary>
        /// <param name="recnum">文档唯一编号</param>
        /// <param name="text">文档内容</param>
        private void AddtoIndex(int recnum, string text)
        {
            if (text == "" || text == null)
                return;
            text = text.ToLowerInvariant(); // lowercase index 转小写
            string[] keys;
            //分词
            if (_docMode)
            {
                //_log.Debug("text size = " + text.Length);
                //分词器
                //Dictionary<string, int> wordfreq = Tokenizer.GenerateWordFreq(text);
                Dictionary<string, int> wordfreq = _tokenizer.GenerateWordFreq(text);
                //_log.Debug("word count = " + wordfreq.Count);
                var kk = wordfreq.Keys;
                keys = new string[kk.Count];
                kk.CopyTo(keys, 0);
            }
            else
            {
                keys = text.Split(' ');
            }
            
            //------------------------------
            //将分词出来的词进行判断
            //已经存在则得到该词对应的位图索引
            //不存在则创建新的索引编号
            //------------------------------
            foreach (string key in keys)
            {
                if (key == "")
                    continue;

                int bmp;
                if (_words.TryGetValue(key, out bmp))//存在
                {
                    _bitmaps.GetBitmap(bmp).Set(recnum, true);
                }
                else
                {
                    //索引编号
                    bmp = _bitmaps.GetFreeRecordNumber();
                    _bitmaps.SetDuplicate(bmp, recnum);
                    _words.Add(key, bmp);
                }
            }
            _wordschanged = true;
        }

    }
}