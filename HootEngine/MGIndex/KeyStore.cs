using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Hoot.Common;

namespace Hoot
{
    
    internal class KeyStoreString : IDisposable
    {
        bool _caseSensitive = false;

        KeyStore<int> _db;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="caseSensitve">是否区分大小写</param>
        public KeyStoreString(string path,string filename, bool caseSensitve)
        {
            _db = KeyStore<int>.Open(Path.Combine(path, filename), true);
            _caseSensitive = caseSensitve;
        }

        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);
            MemoryStream ms = new MemoryStream();
            ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.SetBytes(hc, ms.ToArray());
        }

        public bool Get(string key, out string val)
        {
            val = null;
            byte[] bval;
            bool b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(string key, out byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            val = null;
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);

            if (_db.GetBytes(hc, out val))
            {
                // unpack data
                byte[] g = null;
                if (UnpackData(val, out val, out g))
                {
                    //if (Helper.CompareMemCmp(bkey, g) != 0)
                    if (Helper.CompareMemCmp(bkey, g)==false)
                    {
                        // if data not equal check duplicates (hash conflict)
                        List<int> ints = new List<int>(_db.GetDuplicates(hc));
                        ints.Reverse();
                        foreach (int i in ints)
                        {
                            byte[] bb = _db.FetchRecordBytes(i);
                            if (UnpackData(bb, out val, out g))
                            {
                                //if (Helper.CompareMemCmp(bkey, g) == 0)
                                if (Helper.CompareMemCmp(bkey, g))
                                    return true;
                            }
                        }
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        public int Count()
        {
            return (int)_db.Count();
        }

        public int RecordCount()
        {
            return (int)_db.RecordCount();
        }

        public void SaveIndex()
        {
            _db.SaveIndex();
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
        }

        private bool UnpackData(byte[] buffer, out byte[] val, out byte[] key)
        {
            int len = Helper.ToInt32(buffer, 0, false);
            key = new byte[len];
            Buffer.BlockCopy(buffer, 4, key, 0, len);
            val = new byte[buffer.Length - 4 - len];
            Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);

            return true;
        }

        public string ReadData(int recnumber)
        {
            byte[] val;
            byte[] key;
            byte[] b = _db.FetchRecordBytes(recnumber);
            if (UnpackData(b, out val, out key))
            {
                return Encoding.Unicode.GetString(val);
            }
            return "";
        }

        internal void FreeMemory()
        {
            _db.FreeMemory();
        }
    }

    internal class KeyStore<T> : IDisposable, IDocStorage<T> where T : IComparable<T>
    {
        private ILog log = LogManager.GetLogger(typeof(KeyStore<T>));
        /// <summary>
        /// 路径
        /// </summary>
        private string _Path = "";
        /// <summary>
        /// 文件名
        /// </summary>
        private string _FileName = "";
        /// <summary>
        /// 最大key的大小
        /// </summary>
        private byte _MaxKeySize;
        /// <summary>
        /// 存档文件
        /// </summary>
        private StorageFile<T> _archive;
        /// <summary>
        /// 索引
        /// </summary>
        private MGIndex<T> _index;

        /// <summary>
        /// 后缀 .mgdat
        /// </summary>
        //private string _datExtension = ".mgdat";
        //private string _idxExtension = ".mgidx";
        private string _datExtension = Global.KSdatExt;
        private string _idxExtension = Global.KSidxExt;
        private IGetBytes<T> _T = null;
        private System.Timers.Timer _savetimer;
        private BoolIndex _deleted;
        /// <summary>
        /// 存储锁对象
        /// </summary>
        object _savelock = new object();
        /// <summary>
        /// 关闭锁
        /// </summary>
        private object _shutdownlock = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Filename">文件地址，全路径</param>
        /// <param name="MaxKeySize">键值的长度</param>
        /// <param name="AllowDuplicateKeys">是否允许重复键</param>
        public KeyStore(string Filename, byte MaxKeySize, bool AllowDuplicateKeys)
        {
            Initialize(Filename, MaxKeySize, AllowDuplicateKeys);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Filename"></param>
        /// <param name="AllowDuplicateKeys"></param>
        public KeyStore(string Filename, bool AllowDuplicateKeys)
        {
            Initialize(Filename, Global.DefaultStringKeySize, AllowDuplicateKeys);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Filename"></param>
        /// <param name="AllowDuplicateKeys"></param>
        /// <returns></returns>
        public static KeyStore<T> Open(string Filename, bool AllowDuplicateKeys)
        {
            return new KeyStore<T>(Filename, AllowDuplicateKeys);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Filename"></param>
        /// <param name="MaxKeySize"></param>
        /// <param name="AllowDuplicateKeys"></param>
        /// <returns></returns>
        public static KeyStore<T> Open(string Filename, byte MaxKeySize, bool AllowDuplicateKeys)
        {
            return new KeyStore<T>(Filename, MaxKeySize, AllowDuplicateKeys);
        }

        /// <summary>
        /// 保存索引
        /// </summary>
        public void SaveIndex()
        {
            if (_index == null)
                return;
            lock (_savelock)
            {
                log.Debug("saving to disk");
                _index.SaveIndex();
                _deleted.SaveIndex();
                log.Debug("index saved");
            }
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            // get duplicates from index
            return _index.GetDuplicates(key);
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _archive.ReadBytes(record);
        }

        public long Count()
        {
            int c = _archive.Count();
            return c - _deleted.GetBits().CountOnes() * 2;
        }

        public bool Get(T key, out string val)
        {
            byte[] b = null;
            val = "";
            bool ret = GetBytes(key, out b);
            if (ret)
            {
                if (b != null)
                    val = Encoding.Unicode.GetString(b);
                else
                    val = "";
            }
            return ret;
        }

        public bool GetObject(T key, out object val)
        {
            int off;
            val = null;
            if (_index.Get(key, out off))
            {
                val = _archive.ReadObject(off);
                return true;
            }
            return false;
        }

        public bool GetBytes(T key, out byte[] val)
        {
            int off;
            val = null;
            // search index

            if (_index.Get(key, out off))
            {
                val = _archive.ReadBytes(off);
                return true;
            }
            return false;
        }

        public int SetString(T key, string data)
        {
            return SetBytes(key, Encoding.Unicode.GetBytes(data));
        }

        public int SetObject(T key, object doc)
        {
            int recno = -1;
            // save to storage
            recno = (int) _archive.WriteObject(key, doc);
            // save to index
            _index.Set(key, recno);

            return recno;
        }

        public int SetBytes(T key, byte[] data)
        {
            int recno = -1;
            // save to storage
            recno = (int)_archive.WriteData(key, data);
            // save to index
            _index.Set(key, recno);

            return recno;
        }
        /// <summary>
        /// 关闭
        /// </summary>
        public void Shutdown()
        {
            lock (_shutdownlock)
            {
                if (_index != null)
                    log.Debug("Shutting down");
                else
                    return;
                _savetimer.Enabled = false;
                SaveIndex();
                SaveLastRecord();

                if (_deleted != null)
                    _deleted.Shutdown();
                if (_index != null)
                    _index.Shutdown();
                if (_archive != null)
                    _archive.Shutdown();
                _index = null;
                _archive = null;
                _deleted = null;
                //log.Debug("Shutting down log");
                //LogManager.Shutdown();
            }
        }
        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            Shutdown();
        }

        public int RecordCount()
        {
            return _archive.Count();
        }

        public int[] GetHistory(T key)
        {
            List<int> a = new List<int>();
            foreach (int i in GetDuplicates(key))
            {
                a.Add(i);
            }
            return a.ToArray();
        }

        internal byte[] FetchRecordBytes(int record, out bool isdeleted)
        {
            StorageItem<T> meta;
            byte[] b = _archive.ReadBytes(record, out meta);
            isdeleted = meta.isDeleted;
            return b;
        }

        internal bool Delete(T id)
        {
            // write a delete record
            int rec = (int)_archive.Delete(id);
            _deleted.Set(true, rec);
            return _index.RemoveKey(id);
        }

        internal bool DeleteReplicated(T id)
        {
            // write a delete record for replicated object
            int rec = (int)_archive.DeleteReplicated(id);
            _deleted.Set(true, rec);
            return _index.RemoveKey(id);
        }

        internal int CopyTo(StorageFile<T> storagefile, long startrecord)
        {
            return _archive.CopyTo(storagefile, startrecord);
        }

        public byte[] GetBytes(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadBytes(rowid, out meta);
        }
        /// <summary>
        /// 释放内存
        /// </summary>
        internal void FreeMemory()
        {
            _index.FreeMemory();
        }

        public object GetObject(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadObject(rowid, out meta);
        }

        public StorageItem<T> GetMeta(int rowid)
        {
            return _archive.ReadMeta(rowid);
        }

        private void SaveLastRecord()
        {
            // save the last record number in the index file
            _index.SaveLastRecordNumber(_archive.Count());
        }
        /// <summary>
        /// 初始化
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="maxkeysize"></param>
        /// <param name="AllowDuplicateKeys"></param>
        private void Initialize(string filename, byte maxkeysize, bool AllowDuplicateKeys)
        {
            _MaxKeySize = RDBDataType<T>.GetByteSize(maxkeysize);
            _T = RDBDataType<T>.ByteHandler();

            _Path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(_Path);

            _FileName = Path.GetFileNameWithoutExtension(filename);
            string db = Path.Combine(_Path, _FileName + _datExtension);

            _index = new MGIndex<T>(_Path, _FileName + _idxExtension, _MaxKeySize, AllowDuplicateKeys);

            if (Global.SaveAsBinaryJSON)
                _archive = new StorageFile<T>(db, SF_FORMAT.BSON, false);
            else
                _archive = new StorageFile<T>(db, SF_FORMAT.JSON, false);

            _deleted = new BoolIndex(_Path, _FileName);

            log.Debug("Current Count = " + RecordCount().ToString("#,0"));

            CheckIndexState();
            //保存服务
            log.Debug("Starting save timer");
            _savetimer = new System.Timers.Timer();
            _savetimer.Elapsed += new System.Timers.ElapsedEventHandler(_savetimer_Elapsed);
            _savetimer.Interval = Global.SaveIndexToDiskTimerSeconds * 1000;
            _savetimer.AutoReset = true;
            _savetimer.Start();

        }

        /// <summary>
        /// 改变索引状态
        /// </summary>
        private void CheckIndexState()
        {
            log.Debug("Checking Index state...");
            int last = _index.GetLastIndexedRecordNumber();
            int count = _archive.Count();
            if (last < count)
            {
                //重建索引
                log.Debug("Rebuilding index...");
                log.Debug("   last index count = " + last);
                log.Debug("   data items count = " + count);
                // check last index record and archive record
                //       rebuild index if needed
                for (int i = last; i < count; i++)
                {
                    bool deleted = false;
                    T key = _archive.GetKey(i, out deleted);
                    if (deleted == false)
                        _index.Set(key, i);
                    else
                        _index.RemoveKey(key);

                    if (i % 100000 == 0)
                        log.Debug("100,000 items re-indexed");
                }

                log.Debug("Rebuild index done.");
            }
        }

        void _savetimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            log.Debug("exec save timer:" + e.SignalTime);
            SaveIndex();
        }

    }
}
