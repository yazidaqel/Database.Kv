using FraudDetector.Database.Kv.Engine.Page;
using System.Collections.Generic;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    internal interface IMemoryIndexService
    {

        bool TryAdd(string key, PageLocation pageLocation);
        bool TryGet(string key, out List<PageLocation> pageLocations);
        bool TryRemove(string key, PageLocation pageLocation);
        void Rebuild();

    }
}
