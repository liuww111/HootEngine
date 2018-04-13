namespace Hoot
{
    /// <summary>
    /// 全局配置
    /// </summary>
    public class Global
    {
        /// <summary>
        /// Store bitmap as int offsets then switch over to bitarray
        /// 位图索引总数，超过这个值的时候切换到BitArray
        /// </summary>
        public static int BitmapOffsetSwitchOverCount = 10;
        /// <summary>
        /// Default maximum string key size for indexes
        /// 默认键值词的最大长度
        /// </summary>
        public static byte DefaultStringKeySize = 60;
        /// <summary>
        /// Free bitmap index memory on save 
        /// 释放bitmap内存并存储
        /// </summary>
        public static bool FreeBitmapMemoryOnSave = false;
        /// <summary>
        /// Number of items in each index page (default = 10000) [Expert only, do not change]
        /// 每个索引页中的项目数
        /// </summary>
        public static ushort PageItemCount = 10000;
        /// <summary>
        /// KeyStore save to disk timer
        /// 定时保存到磁盘的间隔秒数
        /// </summary>
        public static int SaveIndexToDiskTimerSeconds = 1800;
        /// <summary>
        /// Flush the StorageFile stream immediately
        /// 存储文件流是否立即刷新
        /// </summary>
        public static bool FlushStorageFileImmediately = false;
        /// <summary>
        /// Save doc as binary json
        /// 是否将文档保存成BSON
        /// </summary>
        public static bool SaveAsBinaryJSON = true;
        /// <summary>
        /// Split the data storage files in MegaBytes (default 0 = off) [500 = 500mb]
        /// <para> - You can set and unset this value anytime and it will operate from that point on.</para>
        /// <para> - If you unset (0) the value previous split files will remain and all the data will go to the last file.</para>
        /// 切割存储的文件0是关闭
        /// </summary>
        public static ushort SplitStorageFilesMegaBytes = 0;
        /// <summary>
        /// Compress the documents in the storage file if it is over this size (default = 100 Kilobytes) 
        /// <para> - You will be trading CPU for disk IO</para>
        /// 压缩文件中存储的文件如果是超过这个大小（默认为100字节）
        /// </summary>
        public static ushort CompressDocumentOverKiloBytes = 100;
        /// <summary>
        /// Disk block size for high frequency KV storage file (default = 2048)
        /// <para> * Do not use anything under 512 with large string keys</para>
        /// 用于高频KV存储文件的磁盘块大小（默认值= 2048）
        /// </summary>
        public static ushort HighFrequencyKVDiskBlockSize = 2048;

        //扩展名
        public static string BmrExt = ".mgbmr";//存储词对应的索引ID号
        public static string BmpExt = ".mgbmp";//存储每个索引ID对应的文档ID列表
        public static string KSdatExt = ".mgdat";//存储文档
        public static string KSidxExt = ".mgidx";//
        public static string BoolIndexExt = ".deleted";//
        public static string WordsExt = ".words";//词库
        public static string STExt = ".mgrec";//存储文档的索引ID（根据文档ID查找文档对应的索引ID，然后再查文档）

    }
}
