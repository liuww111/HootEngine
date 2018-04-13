﻿using System;
using System.Collections.Generic;

namespace Hoot
{
    /// <summary>
    /// 字对齐混合（WAH）压缩BitArrays
    /// 每个索引ID对应的文档ID列表对象
    /// </summary>
    public class WAHBitArray
    {
        public enum TYPE
        {
            WAH = 1,
            Bitarray = 0,
            Indexes = 2
        }

        public WAHBitArray()
        {
            _state = TYPE.Indexes;
        }

        public WAHBitArray(TYPE type, uint[] ints)
        {
            _state = type;
            switch (type)
            {
                case TYPE.WAH:
                    _compressed = ints;
                    Uncompress();
                    _state = TYPE.Bitarray;
                    _compressed = null;
                    break;
                case TYPE.Bitarray:
                    _uncompressed = ints;
                    break;
                case TYPE.Indexes:
                    _offsets = new SortedList<uint, bool>(); 
                            //new Dictionary<uint, bool>();
                    foreach (var i in ints)
                        _offsets.Add(i, true);
                    break;
            }
        }
        /// <summary>
        /// 压缩
        /// </summary>
        private uint[] _compressed;
        /// <summary>
        /// 未经压缩
        /// </summary>
        private uint[] _uncompressed;
        //private Dictionary<uint, bool> _offsets = new Dictionary<uint, bool>();
        /// <summary>
        /// 有序列表，存储文档编号
        /// </summary>
        private SortedList<uint, bool> _offsets = new SortedList<uint, bool>();
        private uint _curMax = 0;
        /// <summary>
        /// 默认Indexes，
        /// </summary>
        private TYPE _state;
        public bool isDirty = false;
        private object _lock = new object();

        public WAHBitArray Copy()
        {
            lock (_lock)
            {
                uint[] i = GetBitArray();
                return new WAHBitArray(TYPE.Bitarray, i);
            }
        }

        public bool Get(int index)
        {
            lock (_lock)
            {
                if (_state == TYPE.Indexes)
                {
                    bool b = false;
                    var f = _offsets.TryGetValue((uint)index, out b);
                    if (f)
                        return b;
                    else
                        return false;
                }
                CheckBitArray();

                Resize(index);

                return internalGet(index);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="index">文档编号</param>
        /// <param name="val"></param>
        public void Set(int index, bool val)
        {
            lock (_lock)
            {
                if (_state == TYPE.Indexes)
                {
                    isDirty = true;

                    if (val == true)
                    {
                        _offsets[(uint)index] = true;
                        // set max
                        if (index > _curMax)
                            _curMax = (uint)index;
                    }
                    else
                    {
                        _offsets.Remove((uint)index);
                    }

                    ChangeTypeIfNeeded();
                    return;
                }
                CheckBitArray();

                Resize(index);

                internalSet(index, val);
            }
        }

        public int Length
        {
            set
            {
                lock (_lock)
                {
                    if (_state == TYPE.Indexes)
                    {
                        // ignore
                        return;
                    }
                    CheckBitArray();
                    int c = value >> 5;
                    c++;
                    if (c > _uncompressed.Length)
                    {
                        uint[] ar = new uint[c];
                        _uncompressed.CopyTo(ar, 0);
                        _uncompressed = ar;
                    }
                }
            }
            get
            {
                if (_state == TYPE.Indexes)
                {
                    if (_offsets.Count == 0) return 0;
                    uint[] k = GetOffsets();

                    uint l = k[k.Length - 1];
                    return (int)l;
                }
                CheckBitArray();
                return _uncompressed.Length << 5;
            }
        }

        #region [  B I T    O P E R T A I O N S  ]
        public WAHBitArray And(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] &= right[i];

                return new WAHBitArray(TYPE.Bitarray, left);
            }
        }

        public WAHBitArray AndNot(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] &= ~right[i];

                return new WAHBitArray(TYPE.Bitarray, left);
            }
        }

        public WAHBitArray Or(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] |= right[i];

                return new WAHBitArray(TYPE.Bitarray, left);
            }
        }

        public WAHBitArray Not(int size)
        {
            lock (_lock)
            {
                this.CheckBitArray();

                uint[] left = this.GetBitArray();
                int c = left.Length;
                int ms = size >> 5;
                if (size - (ms << 5) > 0)
                    ms++; // include remainder
                if (ms > c)
                {
                    var a = new uint[ms];
                    Array.Copy(left, 0, a, 0, c);
                    left = a;
                    c = ms;
                }

                for (int i = 0; i < c; i++)
                    left[i] = ~left[i];

                return new WAHBitArray(TYPE.Bitarray, left);
            }
        }

        public WAHBitArray Xor(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] ^= right[i];

                return new WAHBitArray(TYPE.Bitarray, left);
            }
        }
        #endregion

        private static int BitCount(uint n)
        { // 32-bit recursive reduction using SWAR
            n -= ((n >> 1) & 0x55555555);
            n = (((n >> 2) & 0x33333333) + (n & 0x33333333));
            n = (((n >> 4) + n) & 0x0f0f0f0f);
            return (int)((n * 0x01010101) >> 24);
        }

        public long CountOnes()
        {
            if (_state == TYPE.Indexes)
            {
                return _offsets.Count;
            }

            long c = 0;
            CheckBitArray();

            foreach (uint i in _uncompressed)
                c += BitCount(i);

            return c;
        }

        public long CountZeros()
        {
            if (_state == TYPE.Indexes)
            {
                long ones = _offsets.Count;
                uint[] k = GetOffsets();
                long l = k[k.Length - 1];
                return l - ones;
            }

            CheckBitArray();
            int count = _uncompressed.Length << 5;
            long cc = CountOnes();

            return count - cc;
        }
        /// <summary>
        /// 释放内存
        /// </summary>
        public void FreeMemory()
        {
            if (_state == TYPE.Bitarray)
            {
                if (_uncompressed != null)
                {
                    lock (_lock)
                    {
                        _compressed = Compress(_uncompressed);
                        _uncompressed = null;
                        _state = TYPE.WAH;
                    }
                }
            }
        }

        public uint[] GetCompressed(out TYPE type)
        {
            type = TYPE.WAH;

            ChangeTypeIfNeeded();
            if (_state == TYPE.Indexes)
            {
                type = TYPE.Indexes;
                return GetOffsets();
            }
            else if (_uncompressed == null)
                return new uint[] { 0 };

            uint[] d = Compress(_uncompressed);
            return d;
        }

        public IEnumerable<int> GetBitIndexes()
        {
            if (_state == TYPE.Indexes)
            {
                foreach (int i in GetOffsets())
                    yield return i;
            }
            else
            {
                CheckBitArray();
                int count = _uncompressed.Length;

                for (int i = 0; i < count; i++)
                {
                    if (_uncompressed[i] > 0)
                    {
                        for (int j = 0; j < 32; j++)
                        {
                            bool b = internalGet((i << 5) + j);
                            if (b == true)// ones)
                                yield return (i << 5) + j;
                        }
                    }
                }
            }
        }

        private uint[] GetOffsets()
        {
            uint[] k;
            lock (_lock)
            {
                k = new uint[_offsets.Count];
                _offsets.Keys.CopyTo(k, 0);
            }
            Array.Sort(k);
            return k;
        }

        private void prelogic(WAHBitArray op, out uint[] left, out uint[] right)
        {
            this.CheckBitArray();

            left = this.GetBitArray();
            right = op.GetBitArray();
            int ic = left.Length;
            int uc = right.Length;
            if (ic > uc)
            {
                uint[] ar = new uint[ic];
                right.CopyTo(ar, 0);
                right = ar;
            }
            else if (ic < uc)
            {
                uint[] ar = new uint[uc];
                left.CopyTo(ar, 0);
                left = ar;
            }
        }

        internal uint[] GetBitArray()
        {
            lock (_lock)
            {
                if (_state == TYPE.Indexes)
                    return UnpackOffsets();

                this.CheckBitArray();
                uint[] ui = new uint[_uncompressed.Length];
                _uncompressed.CopyTo(ui, 0);

                return ui;
            }
        }

        private uint[] UnpackOffsets()
        {
            // return bitmap uints 
            uint max = 0;
            if (_offsets.Count == 0) return new uint[0];
            uint[] k = GetOffsets();
            max = k[k.Length - 1];

            uint[] ints = new uint[(max >> 5) + 1];

            foreach (int index in k)
            {
                int pointer = ((int)index) >> 5;
                uint mask = (uint)1 << (31 - // high order bit set
                    ((int)index % 32));

                ints[pointer] |= mask;
            }

            return ints;
        }

        private void ChangeTypeIfNeeded()
        {
            if (_state != TYPE.Indexes)
                return;

            uint T = (_curMax >> 5) + 1;
            int c = _offsets.Count;
            if (c > T && c > Global.BitmapOffsetSwitchOverCount)
            {
                // change type to WAH
                _state = TYPE.Bitarray;
                _uncompressed = new uint[0];
                // create bitmap
                foreach (var i in _offsets.Keys)
                    Set((int)i, true);
                // clear list
                _offsets = new SortedList<uint, bool>();
                 //new Dictionary<uint, bool>();
            }
        }

        private void Resize(int index)
        {
            if (_state == TYPE.Indexes)
                return;
            int c = index >> 5;
            c++;
            if (_uncompressed == null)
            {
                _uncompressed = new uint[c];
                return;
            }
            if (c > _uncompressed.Length)
            {
                uint[] ar = new uint[c];
                _uncompressed.CopyTo(ar, 0);
                _uncompressed = ar;
            }
        }

        private static void ResizeAsNeeded(List<uint> list, int index)
        {
            int count = index >> 5;

            while (list.Count < count)
                list.Add(0);
        }

        private void internalSet(int index, bool val)
        {
            isDirty = true;
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - // high order bit set
                (index % 32));

            if (val)
                _uncompressed[pointer] |= mask;
            else
                _uncompressed[pointer] &= ~mask;
        }

        private bool internalGet(int index)
        {
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - // high order bit get
                (index % 32));

            if (pointer < _uncompressed.Length)
                return (_uncompressed[pointer] & mask) != 0;
            else
                return false;
        }

        private void CheckBitArray()
        {
            if (_state == TYPE.Bitarray)
                return;

            if (_state == TYPE.WAH)
            {
                _uncompressed = new uint[0];
                Uncompress();
                _state = TYPE.Bitarray;
                _compressed = null;
                return;
            }
        }

        private static uint Take31Bits(uint[] data, int index)
        {
            ulong l1 = 0;
            ulong l2 = 0;
            ulong l = 0;
            ulong ret = 0;
            int off = (index % 32);
            int pointer = index >> 5;

            l1 = data[pointer];
            pointer++;
            if (pointer < data.Length)
                l2 = data[pointer];

            l = (l1 << 32) + l2;
            ret = (l >> (33 - off)) & 0x7fffffff;

            return (uint)ret;
        }

        private static uint[] Compress(uint[] data)
        {
            List<uint> compressed = new List<uint>();
            uint zeros = 0;
            uint ones = 0;
            int count = data.Length << 5;
            int i = 0;
            while (i < count)//for (int i = 0; i < count;)
            {
                uint num = Take31Bits(data, i);
                i += 31;
                if (num == 0) // all zero
                {
                    zeros += 31;
                    FlushOnes(compressed, ref ones);
                }
                else if (num == 0x7fffffff) // all ones
                {
                    ones += 31;
                    FlushZeros(compressed, ref zeros);
                }
                else // literal
                {
                    FlushOnes(compressed, ref ones);
                    FlushZeros(compressed, ref zeros);
                    compressed.Add(num);
                }
            }
            FlushOnes(compressed, ref ones);
            FlushZeros(compressed, ref zeros);
            return compressed.ToArray();
        }

        private static void FlushOnes(List<uint> compressed, ref uint ones)
        {
            if (ones > 0)
            {
                uint n = 0xc0000000 + ones;
                ones = 0;
                compressed.Add(n);
            }
        }

        private static void FlushZeros(List<uint> compressed, ref uint zeros)
        {
            if (zeros > 0)
            {
                uint n = 0x80000000 + zeros;
                zeros = 0;
                compressed.Add(n);
            }
        }

        private static void Write31Bits(List<uint> list, int index, uint val)
        {
            ResizeAsNeeded(list, index + 32);

            int off = (index % 32);
            int pointer = index >> 5;

            if (pointer >= list.Count - 1)
                list.Add(0);

            ulong l = ((ulong)list[pointer] << 32) + list[pointer + 1];
            l |= (ulong)val << (33 - off);

            list[pointer] = (uint)(l >> 32);
            list[pointer + 1] = (uint)(l & 0xffffffff);
        }

        private void WriteOnes(List<uint> list, int index, uint count)
        {
            ResizeAsNeeded(list, index);

            int off = index % 32;
            int pointer = index >> 5;
            int ccount = (int)count;
            int indx = index;
            int x = 32 - off;

            if (pointer >= list.Count)
                list.Add(0);

            if (ccount > x )//|| x == 32) //current pointer
            {
                list[pointer] |= (uint)((0xffffffff >> off));
                ccount -= x;
                indx += x;
            }
            else
            {
                list[pointer] |= (uint)((0xffffffff << (32 - ccount)) >> off);
                ccount = 0;
            }

            bool checklast = true;
            while (ccount >= 32)//full ints
            {
                if (checklast && list[list.Count - 1] == 0)
                {
                    list.RemoveAt(list.Count - 1);
                    checklast = false;
                }

                list.Add(0xffffffff);
                ccount -= 32;
                indx += 32;
            }
            int p = indx >> 5;
            off = indx % 32;
            if (ccount > 0)
            {
                uint i = 0xffffffff << (32 - ccount);
                if (p > (list.Count - 1)) //remaining
                    list.Add(i);
                else
                    list[p] |= (uint)(i >> off);
            }
        }
        /// <summary>
        /// 解压缩
        /// </summary>
        private void Uncompress()
        {
            int index = 0;
            List<uint> list = new List<uint>();
            if (_compressed == null)
                return;

            foreach (uint ci in _compressed)
            {
                if ((ci & 0x80000000) == 0) // literal
                {
                    Write31Bits(list, index, ci);
                    index += 31;
                }
                else
                {
                    uint count = ci & 0x3fffffff;
                    if ((ci & 0x40000000) != 0) // ones count
                        WriteOnes(list, index, count);

                    index += (int)count;
                }
            }
            ResizeAsNeeded(list, index);
            _uncompressed = list.ToArray();
        }

        internal static WAHBitArray Fill(int count)
        {
            if (count > 0)
            {
                int c = count >> 5;
                int r = count % 32;
                if (r > 0)
                    c++;
                uint[] ints = new uint[c];
                for (int i = 0; i < c - 1; i++)
                    ints[i] = 0xffffffff;
                ints[c - 1] = 0xffffffff << (31 - r);
                return new WAHBitArray(TYPE.Bitarray, ints);
            }
            return new WAHBitArray();
        }

        internal int GetFirst()
        {
            foreach (var i in GetBitIndexes())
                return i;
            return 0;
        }
    }
}
