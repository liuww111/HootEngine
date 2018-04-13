using System.Collections.Generic;

namespace Hoot.Common
{
    /// <summary>
    /// 安全有序列
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class SafeSortedList<T, V>
    {
        private object _padlock = new object();
        SortedList<T, V> _list = new SortedList<T, V>();

        public int Count
        {
            get { lock (_padlock) return _list.Count; }
        }

        public void Add(T key, V val)
        {
            lock (_padlock)
                if (_list.ContainsKey(key) == false)
                    _list.Add(key, val);
                else
                    _list[key] = val;
        }

        public void Remove(T key)
        {
            if (key == null)
                return;
            lock (_padlock)
                _list.Remove(key);
        }

        public T GetKey(int index)
        {
            lock (_padlock) return _list.Keys[index];
        }

        public V GetValue(int index)
        {
            lock (_padlock) return _list.Values[index];
        }

        public T[] Keys()
        {
            lock (_padlock)
            {
                T[] keys = new T[_list.Keys.Count];
                _list.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public IEnumerator<KeyValuePair<T, V>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<T, V>>)_list).GetEnumerator();
        }

        public bool TryGetValue(T key, out V value)
        {
            lock (_padlock)
                return _list.TryGetValue(key, out value);
        }

        public V this[T key]
        {
            get
            {
                lock (_padlock)
                    return _list[key];
            }
            set
            {
                lock (_padlock)
                    _list[key] = value;
            }
        }
    }

}
