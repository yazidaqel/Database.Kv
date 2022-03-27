
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.File
{
    internal interface IManagedDateFileHelper
    {
        Task WriteAsync(string key, byte[] data, DateTimeOffset appendDateTime);
        IEnumerable<byte[]> Read(string key);

    }
}
