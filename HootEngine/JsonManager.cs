using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace Hoot
{
    public interface IJson
    {
        string SerializeObject(object value);
         T DeserializeObject<T>(string value);
    }
    public interface IBson
    {
        byte[] SerializeObject(object value);
        T DeserializeObject<T>(byte[] value);
    }

    internal class JsonNet : IJson
    {
        public JsonNet()
        {

        }

        public string SerializeObject(object value)
        {
            return JsonConvert.SerializeObject(value);
        }

        public T DeserializeObject<T>(string value)
        {
            return JsonConvert.DeserializeObject<T>(value);
        }

        
}

    internal class BsonNet : IBson
    {
        public T DeserializeObject<T>(byte[] value)
        {
            MemoryStream ms = new MemoryStream(value);
            using (BsonDataReader reader = new BsonDataReader(ms))
            {
                JsonSerializer serializer = new JsonSerializer();
                return serializer.Deserialize<T>(reader);
            }
        }

        public byte[] SerializeObject(object value)
        {
            MemoryStream ms = new MemoryStream();
            using (BsonDataWriter writer = new BsonDataWriter(ms))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(writer, value);
            }
            return ms.ToArray();
        }
    }
    /// <summary>
    /// Json管理器，方便更换Json库
    /// </summary>
    public static class JsonManager
    {
        public static IJson GetJsonNet()
        {
            return new JsonNet();
        }
        public static IBson GetBsonNet()
        {
            return new BsonNet();
        }
    }
}
