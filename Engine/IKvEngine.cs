using System.Collections.Generic;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine
{
    internal interface IKvEngine
    {
        Task AppendAsync<T>(string key, T value) where T : class;
        Task<IEnumerable<T>> GetAsync<T>(string key) where T : class;

    }
}
