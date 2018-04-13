using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Hoot.Common
{
    internal static class Helper
    {
        public static MurmurHash2Unsafe MurMur = new MurmurHash2Unsafe();
        public static bool CompareMemCmp(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            //
            for (int i=0; i<c;i++) {
                if (left[i] != right[i])
                {
                    return false;
                }
            }
            return true;
            //
            //return memcmp(left, right, c);
        }

        //[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        //private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);



        internal static unsafe int ToInt32(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[4];
                Buffer.BlockCopy(value, startIndex, b, 0, 4);
                Array.Reverse(b);
                return ToInt32(b, 0);
            }

            return ToInt32(value, startIndex);
        }

        internal static unsafe int ToInt32(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *((int*)numRef);
            }
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[8];
                Buffer.BlockCopy(value, startIndex, b, 0, 8);
                Array.Reverse(b);
                return ToInt64(b, 0);
            }
            return ToInt64(value, startIndex);
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((long*)numRef));
            }
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[2];
                Buffer.BlockCopy(value, startIndex, b, 0, 2);
                Array.Reverse(b);
                return ToInt16(b, 0);
            }
            return ToInt16(value, startIndex);
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((short*)numRef));
            }
        }

        internal static unsafe byte[] GetBytes(long num, bool reverse)
        {
            byte[] buffer = new byte[8];
            fixed (byte* numRef = buffer)
            {
                *((long*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(int num, bool reverse)
        {
            byte[] buffer = new byte[4];
            fixed (byte* numRef = buffer)
            {
                *((int*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(short num, bool reverse)
        {
            byte[] buffer = new byte[2];
            fixed (byte* numRef = buffer)
            {
                *((short*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        internal static string GetString(byte[] buffer, int index, short keylength)
        {
            return Encoding.UTF8.GetString(buffer, index, keylength);
        }
    }
}
