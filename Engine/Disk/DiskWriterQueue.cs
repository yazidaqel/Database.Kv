using FraudDetector.Database.Kv.Engine.File;
using FraudDetector.Database.Kv.Engine.Memory;
using FraudDetector.Database.Kv.Engine.Page;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MsgPack.Serialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.Disk
{
    internal class DiskWriterQueue : BackgroundService
    {

        private readonly ILogger<DiskWriterQueue> _logger;
        private readonly ChannelReader<PageBuffer> _reader;
        private readonly MessagePackSerializer<PageBuffer> _serializer;
        private readonly IManagedDateFileHelper _managedDateFileHelper;
        private readonly IMemoryCache _memoryCache;

        private bool _writing = false;


        private bool _disposed = false;

        public DiskWriterQueue(
            ILogger<DiskWriterQueue> logger,
            IMemoryQueueProvider<PageBuffer> channelDataProvider,
            IManagedDateFileHelper managedDateFileHelper,
            IMemoryCache memoryCache)
        {
            _reader = channelDataProvider.GetChannelReader();
            _logger = logger;
            _serializer = MessagePackSerializer.Get<PageBuffer>();
            _managedDateFileHelper = managedDateFileHelper;
            _memoryCache = memoryCache;

        }



        private async Task FlushToDiskAsync(PageBuffer item)
        {
            if (item == null)
                return;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            byte[] data = await _serializer.PackSingleObjectAsync(item);

            await _managedDateFileHelper.WriteAsync(item.Key, data, item.AppendedAt);

            stopwatch.Stop();
            _logger.LogDebug("Persist Key={Key} to disk, ElapsedMilliseconds={ElapsedMilliseconds}", item.Key, stopwatch.ElapsedMilliseconds);

        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting background persistence job");
            while (!stoppingToken.IsCancellationRequested)
            {
                while (await _reader.WaitToReadAsync())
                {
                    _writing = true;
                    if (_reader.TryRead(out PageBuffer item))
                    {
                        try
                        {
                            await FlushToDiskAsync(item);

                            _logger.LogDebug("Removing {Key} Saved to disk from cache", item.Key);
                            // Maybe locks are needed around this operation, 
                            // but a new DI is needed
                            if (_memoryCache.TryGetValue(item.Key, out List<PageBuffer> values) && values != null)
                            {
                                PageBuffer pageToRemove = values.SingleOrDefault(x => x.Equals(item));
                                if(pageToRemove != null)
                                    values.Remove(pageToRemove);
                                _memoryCache.Set(item.Key, values);
                                _logger.LogDebug("{Key} has {Count} Page Buffer in cache after saving to disk", item.Key, values.Count);
                            }

                        }
                        catch (Exception ex)
                        {
                            _logger.LogCritical("Critical exception has been occured, unable to write to disk: {ex}", ex);
                        }
                    }
                    _writing = false;
                }
            }
        }

        public override void Dispose()
        {
            DisposingAsync(true).GetAwaiter().GetResult();
            base.Dispose();
        }


        protected virtual async Task DisposingAsync(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                while (_writing)
                {
                    // Wait for some seconds before disposing this thread
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
            _disposed = true;
        }

    }
}
