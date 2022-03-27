using FraudDetector.Database.Kv.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    public class RetentionPeriodCleaner : IHostedService, IDisposable
    {

        private readonly ILogger _logger;
        private readonly int _retentionPeriod;
        private bool _disposed = false;
        private Timer _timer;

        public RetentionPeriodCleaner(
            IOptions<KvConfiguration> options,
            ILogger<RetentionPeriodCleaner> logger
            )
        {
            _logger = logger;
            _retentionPeriod = options.Value.RetentionPeriod;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start Retention Period Manager");

            _timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromDays(1));

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Stop Retention Period Manager");

            _timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
        }

        private void DoWork(object stateInfo)
        {
            try
            {
                // Looping over the files and remove all items
            }catch(Exception ex)
            {
                _logger.LogError("An exception has occured during retention period cleaning, Error={ex}", ex);
            }

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
