using FraudDetector.Database.Kv.Client;
using FraudDetector.Database.Kv.Engine.File;
using FraudDetector.Database.Kv.Engine.Memory;
using FraudDetector.Database.Kv.Engine.Page;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MsgPack.Serialization;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.Disk
{
    internal class DiskService
    {
        // Channel used to write data to disk

        private readonly ChannelWriter<PageBuffer> _writer;
        private readonly ILogger<DiskService> _logger;
        private readonly MessagePackSerializer<PageBuffer> _serializer;
        private readonly IManagedDateFileHelper _managedDateFileHelper;

        private readonly IMemoryCache _memoryCache;


        public DiskService(
            IMemoryQueueProvider<PageBuffer> channelDataProvider,
            IMemoryCache memoryCache,
            ILogger<DiskService> logger,
            IManagedDateFileHelper managedDateFileHelper)
        {
            _writer = channelDataProvider.GetChannelWriter();
            _logger = logger;
            _serializer = MessagePackSerializer.Get<PageBuffer>();
            _managedDateFileHelper = managedDateFileHelper;
            _memoryCache = memoryCache;

        }

        public async Task WriteAsync(PageBuffer pageBuffer)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            _logger.LogDebug("Adding {Key} data to cache", pageBuffer.Key);
            if(!_memoryCache.TryGetValue(pageBuffer.Key, out List<PageBuffer> values) || values == null)
            {
                values = new List<PageBuffer>();
            }
            values.Add(pageBuffer);
            _memoryCache.Set(pageBuffer.Key, values);

            _logger.LogDebug("{Key} has {Count} Page Buffer in cache", pageBuffer.Key, values.Count);

            _logger.LogDebug("Queuing {Key} data to be saved on disk", pageBuffer.Key);
            while (await _writer.WaitToWriteAsync())
            {
                _logger.LogDebug("Queue is open for writing", pageBuffer.Key);
                if (_writer.TryWrite(pageBuffer)) return;
            }
            stopwatch.Stop();
            _logger.LogDebug("ElapsedMilliseconds={ElapsedMilliseconds} to wait to write Key={Key}", stopwatch.ElapsedMilliseconds, pageBuffer.Key);
        }

        public IEnumerable<PageBuffer> ReadAsync(string key)
        {
            _logger.LogDebug("Reading data by Key={key} from persisted files", key);

            IEnumerable<PageBuffer> fromDisk = _managedDateFileHelper.Read(key).Select(item => _serializer.UnpackSingleObject(item));

            if (_memoryCache.TryGetValue(key, out List<PageBuffer> values) && values != null && values.Count > 0)
            {
                _logger.LogDebug("Reading data by Key={key} from cache", key);
                return fromDisk.Concat(values);
            }

            return fromDisk;
        }
    }
}
