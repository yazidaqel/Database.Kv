using FraudDetector.Database.Kv.Engine.Disk;
using FraudDetector.Database.Kv.Engine.Page;
using MsgPack.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine
{
    internal class KvEngine : IKvEngine
    {

        private readonly DiskService _diskService;

        public KvEngine(DiskService diskService)
        {
            _diskService = diskService;
        }

        public Task AppendAsync<T>(string key, T value) where T : class
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(value));

            var serializer = MessagePackSerializer.Get<T>();

            return Task.Run(async () =>
            {
                await _diskService.WriteAsync(new PageBuffer()
                {
                    Key = key,
                    AppendedAt = DateTimeOffset.Now,
                    Value = await serializer.PackSingleObjectAsync(value)
                });

            });

        }

        public Task<IEnumerable<T>> GetAsync<T>(string key) where T : class
        {
            return Task.Run(async () =>
            {
                var serializer = MessagePackSerializer.Get<T>();
                List<T> objects = new List<T>();
                foreach (var item in _diskService.ReadAsync(key))
                {
                    if (item.Value != null && item.Value.Length > 0)
                        objects.Add(await serializer.UnpackSingleObjectAsync(item.Value));
                }

                return objects.AsEnumerable();
            });
        }
    }
}
