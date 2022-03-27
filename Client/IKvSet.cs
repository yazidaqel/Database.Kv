using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Client
{
    public interface IKvSet
    {

        bool TryAppend<T>(string key, T value) where T : class;

        Task AppendAsync<T>(string key, T value) where T : class;

        bool TryGet<T>(string key, out IEnumerable<T> values) where T : class;

        bool TryGet<T>(string key, TimeSpan period, out IEnumerable<T> values) where T : class;

        Task<IEnumerable<T>> GetAsync<T>(string key) where T : class;

        Task<IEnumerable<T>> GetAsync<T>(string key, TimeSpan period) where T : class;

    }
}
