using System;

namespace FraudDetector.Database.Kv.Client
{
    public class KvConfiguration
    {
        public int RetentionPeriod { get; set; } = 7;
        public string DbPath { get; set; } = $"{AppContext.BaseDirectory}KvStore";
        public int ChunkSize { get; set; } = 1024;
        public int MaxFileSize { get; set; } = 1024 * 1000;
        public int HeaderSize { get; set; } = 128;
        public int IndexRebuildPeriod { get; set; } = 24;

    }
}
