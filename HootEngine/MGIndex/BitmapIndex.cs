using System;
using System.Collections.Generic;
using System.IO;
using Hoot.Common;
using System.Threading;

namespace Hoot
{
    /// <summary>
    /// 位图索引
    /// https://www.cnblogs.com/LBSer/p/3322630.html
    /// </summary>
    internal class BitmapIndex
    {
        public BitmapIndex(string path, string filename)
        {
            _FileName = Path.GetFileNameWithoutExtension(filename);
            _Path = path;
            //if (_Path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
            //    _Path += Path.DirectorySeparatorChar.ToString();

            Initialize();
        }
        /// <summary>
        /// 用于上锁
        /// </summary>
        class L : IDisposable
        {
            BitmapIndex _sc;
            public L(BitmapIndex sc)
            {
                _sc = sc;
                _sc.CheckInternalOP();
            }
            void IDisposable.Dispose()
            {
                _sc.Done();
            }
        }

        //private string _recExt = ".mgbmr";
        //private string _bmpExt = ".mgbmp";
        private string _recExt = Global.BmrExt;
        private string _bmpExt = Global.BmpExt;

        private string _FileName = "";
        private string _Path = "";
        private FileStream _bitmapFileWriteOrg;
        private BufferedStream _bitmapFileWrite;
        private FileStream _bitmapFileRead;
        /// <summary>
        /// 索引存储，read
        /// </summary>
        private FileStream _recordFileRead;
        /// <summary>
        /// 索引存储，write
        /// </summary>
        private FileStream _recordFileWriteOrg;
        /// <summary>
        /// 索引存储，缓存
        /// </summary>
        private BufferedStream _recordFileWrite;
        private long _lastBitmapOffset = 0;
        private int _lastRecordNumber = 0;
        //private SafeDictionary<int, WAHBitArray> _cache = new SafeDictionary<int, WAHBitArray>();
        private SafeSortedList<int, WAHBitArray> _cache = new SafeSortedList<int, WAHBitArray>();
        //private SafeDictionary<int, long> _offsetCache = new SafeDictionary<int, long>();
        private ILog log = LogManager.GetLogger(typeof(BitmapIndex));
        private bool _stopOperations = false;
        private bool _shutdownDone = false;
        /// <summary>
        /// 当前真正工作的索引数
        /// </summary>
        private int _workingCount = 0;
        /// <summary>
        /// 是否变化
        /// </summary>
        private bool _isDirty = false;

        private object _oplock = new object();
        private object _readlock = new object();
        private object _writelock = new object();

        public void Shutdown()
        {
            using (new L(this))
            {
                log.Debug("Shutdown BitmapIndex");

                InternalShutdown();
            }
        }

        public int GetFreeRecordNumber()
        {
            using (new L(this))
            {
                int i = _lastRecordNumber++;

                _cache.Add(i, new WAHBitArray());
                return i;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="freeMemory">是否释放内存</param>
        public void Commit(bool freeMemory)
        {
            if (_isDirty == false)
                return;
            using (new L(this))
            {
                log.Debug("writing "+_FileName);
                int[] keys = _cache.Keys();
                Array.Sort(keys);

                foreach (int k in keys)
                {
                    WAHBitArray bmp = null;
                    if (_cache.TryGetValue(k, out bmp) && bmp.isDirty)
                    {
                        this.SaveBitmap(k, bmp);
                        bmp.FreeMemory();
                        bmp.isDirty = false;
                    }
                }
                Flush();
                if (freeMemory)
                {
                    _cache = //new SafeDictionary<int, WAHBitArray>();
                        new SafeSortedList<int, WAHBitArray>();
                    log.Debug("  freeing cache");
                }
                _isDirty = false;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="bitmaprecno">索引编号</param>
        /// <param name="record">文档编号</param>
        public void SetDuplicate(int bitmaprecno, int record)
        {
            using (new L(this))
            {
                WAHBitArray ba = null;

                ba = internalGetBitmap(bitmaprecno); //GetBitmap(bitmaprecno);

                ba.Set(record, true);
                _isDirty = true;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="recno">索引编号</param>
        /// <returns></returns>
        public WAHBitArray GetBitmap(int recno)
        {
            using (new L(this))
            {
                return internalGetBitmap(recno);
            }
        }
        /// <summary>
        /// 优化操作
        /// </summary>
        public void Optimize()
        {
            lock (_oplock)
                lock (_readlock)
                    lock (_writelock)
                    {
                        _stopOperations = true;
                        while (_workingCount > 0) Thread.SpinWait(1);
                        Flush();

                        if (File.Exists(_Path + _FileName + "$" + _bmpExt))
                            File.Delete(_Path + _FileName + "$" + _bmpExt);

                        if (File.Exists(_Path + _FileName + "$" + _recExt))
                            File.Delete(_Path + _FileName + "$" + _recExt);

                        Stream _newrec = new FileStream(_Path + _FileName + "$" + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                        Stream _newbmp = new FileStream(_Path + _FileName + "$" + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                        long newoffset = 0;
                        int c = (int)(_recordFileRead.Length / 8);
                        for (int i = 0; i < c; i++)
                        {
                            long offset = ReadRecordOffset(i);

                            byte[] b = ReadBMPData(offset);
                            if (b == null)
                            {
                                _stopOperations = false;
                                throw new Exception("bitmap index file is corrupted");
                            }

                            _newrec.Write(Helper.GetBytes(newoffset, false), 0, 8);
                            newoffset += b.Length;
                            _newbmp.Write(b, 0, b.Length);

                        }
                        _newbmp.Flush();
                        _newbmp.Close();
                        _newrec.Flush();
                        _newrec.Close();

                        InternalShutdown();

                        File.Delete(_Path + _FileName + _bmpExt);
                        File.Delete(_Path + _FileName + _recExt);
                        File.Move(_Path + _FileName + "$" + _bmpExt, _Path + _FileName + _bmpExt);
                        File.Move(_Path + _FileName + "$" + _recExt, _Path + _FileName + _recExt);

                        Initialize();
                        _stopOperations = false;
                    }
        }

        internal void FreeMemory()
        {
            try
            {
                List<int> free = new List<int>();
                foreach (var b in _cache)
                {
                    if (b.Value.isDirty == false)
                        free.Add(b.Key);
                }
                log.Debug("releasing bmp count = " + free.Count + " out of " + _cache.Count);
                foreach (int i in free)
                    _cache.Remove(i);
            }
            catch (Exception ex){
                log.Error(ex);
            }
        }

        private byte[] ReadBMPData(long offset)
        {
            _bitmapFileRead.Seek(offset, SeekOrigin.Begin);

            byte[] b = new byte[8];

            _bitmapFileRead.Read(b, 0, 8);
            if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
            {
                int c = Helper.ToInt32(b, 2) * 4 + 8;
                byte[] data = new byte[c];
                _bitmapFileRead.Seek(offset, SeekOrigin.Begin);
                _bitmapFileRead.Read(data, 0, c);
                return data;
            }
            return null;
        }
        /// <summary>
        /// 根据编号读取索引位置
        /// </summary>
        /// <param name="recnum"></param>
        /// <returns></returns>
        private long ReadRecordOffset(int recnum)
        {
            byte[] b = new byte[8];
            long off = ((long)recnum) * 8;
            _recordFileRead.Seek(off, SeekOrigin.Begin);
            _recordFileRead.Read(b, 0, 8);
            return Helper.ToInt64(b, 0);
        }
        /// <summary>
        /// 初始化
        /// </summary>
        private void Initialize()
        {
            _recordFileRead = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWriteOrg = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWrite = new BufferedStream(_recordFileWriteOrg);

            _bitmapFileRead = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWriteOrg = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWrite = new BufferedStream(_bitmapFileWriteOrg);

            _bitmapFileWrite.Seek(0L, SeekOrigin.End);
            _lastBitmapOffset = _bitmapFileWrite.Length;
            _lastRecordNumber = (int)(_recordFileRead.Length / 8);
            _shutdownDone = false;
        }

        private void InternalShutdown()
        {
            bool d1 = false;
            bool d2 = false;

            if (_shutdownDone == false)
            {
                Flush();
                if (_recordFileWrite.Length == 0) d1 = true;
                if (_bitmapFileWrite.Length == 0) d2 = true;
                _recordFileRead.Close();
                _bitmapFileRead.Close();
                _bitmapFileWriteOrg.Close();
                _recordFileWriteOrg.Close();
                _recordFileWrite.Close();
                _bitmapFileWrite.Close();
                if (d1)
                    File.Delete(_Path + _FileName + _recExt);
                if (d2)
                    File.Delete(_Path + _FileName + _bmpExt);
                _recordFileWrite = null;
                _recordFileRead = null;
                _bitmapFileRead = null;
                _bitmapFileWrite = null;
                _recordFileRead = null;
                _recordFileWrite = null;
                _shutdownDone = true;
            }
        }
        /// <summary>
        /// 刷新缓冲区
        /// </summary>
        private void Flush()
        {
            if (_shutdownDone)
                return;

            if (_recordFileWrite != null)
                _recordFileWrite.Flush();

            if (_bitmapFileWrite != null)
                _bitmapFileWrite.Flush();

            if (_recordFileRead != null)
                _recordFileRead.Flush();

            if (_bitmapFileRead != null)
                _bitmapFileRead.Flush();

            if (_bitmapFileWriteOrg != null)
                _bitmapFileWriteOrg.Flush();

            if (_recordFileWriteOrg != null)
                _recordFileWriteOrg.Flush();
        }
        /// <summary>
        /// 内部获取位图
        /// </summary>
        /// <param name="recno"></param>
        /// <returns></returns>
        private WAHBitArray internalGetBitmap(int recno)
        {
            lock (_readlock)
            {
                WAHBitArray ba = new WAHBitArray();
                if (recno == -1)//-1等于不存在，立即创建返回
                    return ba;

                if (_cache.TryGetValue(recno, out ba))
                {
                    return ba;
                }
                else
                {
                    long offset = 0;
                    //if (_offsetCache.TryGetValue(recno, out offset) == false)
                    {
                        offset = ReadRecordOffset(recno);
                       // _offsetCache.Add(recno, offset);
                    }
                    ba = LoadBitmap(offset);

                    _cache.Add(recno, ba);

                    return ba;
                }
            }
        }

        private void SaveBitmap(int recno, WAHBitArray bmp)
        {
            lock (_writelock)
            {
                long offset = SaveBitmapToFile(bmp);
                //long v;
                //if (_offsetCache.TryGetValue(recno, out v))
                //    _offsetCache[recno] = offset;
                //else
                //    _offsetCache.Add(recno, offset);

                long pointer = ((long)recno) * 8;
                _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
                byte[] b = new byte[8];
                b = Helper.GetBytes(offset, false);
                _recordFileWrite.Write(b, 0, 8);
            }
        }


        /// <summary>
        /// 保存到文件
        /// -----------------------------------------------------------------
        /// BITMAP FILE FORMAT
        ///   0  'B','M'
        ///   2  uint count = 4 bytes
        ///   6  Bitmap type :
        ///               0 = int record list   
        ///               1 = uint bitmap
        ///               2 = rec# indexes
        ///   7  '0'
        ///   8  uint data
        /// -----------------------------------------------------------------
        /// </summary>
        /// <param name="bmp"></param>
        /// <returns>文件中的位置</returns>
        private long SaveBitmapToFile(WAHBitArray bmp)
        {
            long off = _lastBitmapOffset;
            WAHBitArray.TYPE t;
            uint[] bits = bmp.GetCompressed(out t);

            byte[] b = new byte[bits.Length * 4 + 8];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Helper.GetBytes(bits.Length, false), 0, b, 2, 4);

            b[6] = (byte)t;
            b[7] = (byte)(0);

            for (int i = 0; i < bits.Length; i++)
            {
                byte[] u = Helper.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 8, 4);
            }
            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            return off;
        }
        /// <summary>
        /// 加载位图
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        private WAHBitArray LoadBitmap(long offset)
        {
            WAHBitArray bc = new WAHBitArray();
            if (offset == -1)
                return bc;

            List<uint> ar = new List<uint>();
            WAHBitArray.TYPE type = WAHBitArray.TYPE.WAH;
            FileStream bmp = _bitmapFileRead;
            {
                bmp.Seek(offset, SeekOrigin.Begin);

                byte[] b = new byte[8];

                bmp.Read(b, 0, 8);//读取头部
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)//验证
                {
                    type = (WAHBitArray.TYPE)Enum.ToObject(typeof(WAHBitArray.TYPE), b[6]);
                    int c = Helper.ToInt32(b, 2);
                    byte[] buf = new byte[c * 4];
                    bmp.Read(buf, 0, c * 4);
                    for (int i = 0; i < c; i++)
                    {
                        ar.Add((uint)Helper.ToInt32(buf, i * 4));
                    }
                }
            }
            bc = new WAHBitArray(type, ar.ToArray());

            return bc;
        }
        /// <summary>
        /// 检查实例操作
        /// </summary>
        private void CheckInternalOP()
        {
            if (_stopOperations)
                lock (_oplock) { } // yes! this is good 检查锁是否被使用
            Interlocked.Increment(ref _workingCount);
        }
        /// <summary>
        /// 完成
        /// </summary>
        private void Done()
        {
            Interlocked.Decrement(ref _workingCount);
        }
    }
}
