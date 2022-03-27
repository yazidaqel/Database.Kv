using FraudDetector.Database.Kv.Engine;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Client
{
    internal class KvSet : IKvSet
    {
        private readonly IKvEngine _kvEngine;
        private readonly ILogger _logger;

        public KvSet(IKvEngine kvEngine,
            ILogger<KvSet> logger
            )
        {
            _kvEngine = kvEngine;
            _logger = logger;
        }

        public bool TryAppend<T>(string key, T value) where T : class
        {
            try
            {
                _kvEngine.AppendAsync(key, value).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception occured during append operation, error={ex}", ex);
                return false;
            }
        }

        public async Task AppendAsync<T>(string key, T value) where T : class
        {
            try
            {
                await _kvEngine.AppendAsync(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception occured during append operation, error={ex}", ex);
            }
        }

        public bool TryGet<T>(string key, out IEnumerable<T> values) where T : class
        {
            try
            {
                values = _kvEngine.GetAsync<T>(key).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Exception occured during get operation, error={ex}", ex);
                values = null;
                return false;
            }
        }

        public bool TryGet<T>(string key, TimeSpan period, out IEnumerable<T> values) where T : class
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<T>> GetAsync<T>(string key) where T : class
        {
            try
            {
                return _kvEngine.GetAsync<T>(key);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Exception occured during get operation, error={ex}", ex);
                return Task.FromResult(Enumerable.Empty<T>());
            }

        }

        public Task<IEnumerable<T>> GetAsync<T>(string key, TimeSpan period) where T : class
        {
            throw new NotImplementedException();
        }




    }
}
