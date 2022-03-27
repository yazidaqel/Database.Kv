using FraudDetector.Database.Kv.Client;
using FraudDetector.Database.Kv.Engine.Page;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    internal class MemoryIndexService : IMemoryIndexService
    {
        private readonly ConcurrentDictionary<string, List<PageLocation>> _indexes = new ConcurrentDictionary<string, List<PageLocation>>();

        private readonly ILogger _logger;
        private readonly string _dbPath;
        private readonly int _chunkSize;

        public MemoryIndexService(
            ILogger<MemoryIndexService> logger,
            IOptions<KvConfiguration> options
            )
        {
            _logger = logger;
            _dbPath = options.Value.DbPath;

            _chunkSize = options.Value.ChunkSize;
        }

        public void Rebuild()
        {
            _logger.LogDebug("Rebuilding indexes has been triggered");
            Stopwatch rebuildTiming = new Stopwatch();
            rebuildTiming.Start();

            // This part may be removed from this class
            // Maybe mediator pattern can be used in this case
            // It should be easy to implement by just adding the notification handler
            try
            {
                IEnumerable<FileInfo> files = Directory.EnumerateFiles(_dbPath, "chunk-*.kv", SearchOption.AllDirectories).Select(f => new FileInfo(f)).OrderBy(x => x.CreationTime);

                foreach (var file in files)
                {
                    using (var fileStream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                    {
                        int position = 0;

                        while (position < fileStream.Length)
                        {
                            byte[] chunk = BufferPool.Instance.Rent(_chunkSize);
                            int bytesRead = fileStream.Read(chunk, 0, _chunkSize);
                            if (bytesRead == _chunkSize)
                            {
                                int keyLength = chunk[4];
                                string keyRead = Encoding.UTF8.GetString(chunk, 5, keyLength);
                                TryAdd(keyRead, new PageLocation()
                                {
                                    IsPersisted = true,
                                    PersistenceFileLocation = file.FullName,
                                    Position = position
                                });
                            }
                            BufferPool.Instance.Return(chunk);
                            position += _chunkSize;
                            fileStream.Seek(position, SeekOrigin.Begin);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogCritical("Error while rebuilding indexes, Error={ex}", ex);
            }

            rebuildTiming.Stop();
            _logger.LogDebug("Rebuilding indexes has finished, ElapsedMilliseconds={ElapsedMilliseconds}", rebuildTiming.ElapsedMilliseconds);
        }

        public bool TryAdd(string key, PageLocation pageLocation)
        {
            _logger.LogDebug("Adding to key={key}", key);

            if (!_indexes.TryGetValue(key, out List<PageLocation> pageLocations) || pageLocations == null)
            {
                _logger.LogDebug("First indexation of {key}", key);

                pageLocations = new List<PageLocation>()
                {
                    pageLocation
                };
                return _indexes.TryAdd(key, pageLocations);
            }
            if (pageLocations.Any(x => x.Equals(pageLocation)))
                return true;

            pageLocations.Add(pageLocation);
            return true;
        }

        public bool TryGet(string key, out List<PageLocation> pageLocations)
        {
            if (!_indexes.TryGetValue(key, out pageLocations) || pageLocations == null)
            {
                return false;
            }
            return true;
        }

        public bool TryRemove(string key, PageLocation pageLocation)
        {
            if (!_indexes.TryGetValue(key, out List<PageLocation> pageLocations) || pageLocations == null)
            {
                return true;
            }
            pageLocations.Remove(pageLocation);
            return true;
        }
    }
}
