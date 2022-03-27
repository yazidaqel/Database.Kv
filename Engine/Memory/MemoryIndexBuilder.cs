using FraudDetector.Database.Kv.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    internal class MemoryIndexBuilder : IHostedService, IDisposable
    {
        private readonly IMemoryIndexService _memoryIndexService;
        private readonly ILogger _logger;
        private Timer _timer;

        private readonly int _rebuildPeriod;

        private bool _disposed = false;



        public MemoryIndexBuilder(
            IMemoryIndexService memoryIndexService,
            IOptions<KvConfiguration> options,
            ILogger<MemoryIndexBuilder> logger
            )
        {
            _logger = logger;
            _memoryIndexService = memoryIndexService;

            _rebuildPeriod = options.Value.IndexRebuildPeriod;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start memory index builder task");

            _timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromHours(_rebuildPeriod));

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Stop memory index builder task");

            _timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
        }

        private void DoWork(object stateInfo)
        {
            try
            {
                _memoryIndexService.Rebuild();
            }
            catch (Exception ex)
            {
                _logger.LogCritical("Error while rebuilding memory indexes, Ex={ex}", ex);
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _timer?.Dispose();
            }
            _disposed = true;
        }
    }
}
