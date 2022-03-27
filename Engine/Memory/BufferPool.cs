using System;
using System.Buffers;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    public class BufferPool
    {
        private readonly object _lock;
        private readonly ArrayPool<byte> _bytePool;

        private BufferPool()
        {
            _lock = new object();
            _bytePool = ArrayPool<byte>.Shared;
        }

        private static readonly Lazy<BufferPool> lazy = new Lazy<BufferPool>(() => new BufferPool());
        public static BufferPool Instance
        {
            get
            {
                return lazy.Value;
            }
        }

        public byte[] Rent(int count)
        {
            lock (_lock)
            {
                return _bytePool.Rent(count);
            }
        }

        public void Return(byte[] bytes)
        {
            lock (_lock)
            {
                _bytePool.Return(bytes);
            }
        }


    }
}
