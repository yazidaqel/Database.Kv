using FraudDetector.Database.Kv.Client;
using FraudDetector.Database.Kv.Engine.Memory;
using FraudDetector.Database.Kv.Engine.Page;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FraudDetector.Database.Kv.Engine.File
{
    internal class ManagedDateFileHelper : IManagedDateFileHelper
    {

        private readonly ILogger<ManagedDateFileHelper> _logger;
        private readonly IMemoryIndexService _memoryIndexService;

        private readonly int _headerSize;
        private readonly int _chunkSize;
        private readonly int _maxFileSize;
        private readonly string _path;

        public ManagedDateFileHelper(
            IOptions<KvConfiguration> options,
            IMemoryIndexService memoryIndexService,
            ILogger<ManagedDateFileHelper> logger)
        {
            _path = options.Value.DbPath;
            _headerSize = options.Value.HeaderSize;
            _chunkSize = options.Value.ChunkSize;
            _maxFileSize = options.Value.MaxFileSize;

            _logger = logger;
            _memoryIndexService = memoryIndexService;
        }

        public IEnumerable<byte[]> Read(string key)
        {
            if (!_memoryIndexService.TryGet(key, out List<PageLocation> fileLocations))
            {
                _logger.LogWarning("There is no saved index using that key={key}", key);
                yield break;
            }
            else
            {
                // Grouping by position to reduce the number of 
                // open/close of files
                var filesSubjectOfRead = fileLocations
                    .Where(f => f.IsPersisted = true)
                    .GroupBy(
                        x => x.PersistenceFileLocation,
                        x => x.Position,
                        (k, g) => new { FileInfo = new FileInfo(k), Positions = g.ToList() }
                    )
                    .OrderBy(x => x.FileInfo.CreationTime);

                foreach (var file in filesSubjectOfRead)
                {
                    using (var fileStream = file.FileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        foreach(int position in file.Positions)
                        {
                            byte[] chunk = BufferPool.Instance.Rent(_chunkSize);

                            fileStream.Seek(position, SeekOrigin.Begin);
                            int bytesRead = fileStream.Read(chunk, 0, _chunkSize);

                            // bytesRead should be equal to the chunk size
                            // Otherwise another write of that chunk is ongoing
                            // No need to read that data

                            if (bytesRead == _chunkSize)
                            {
                                Span<byte> contentLength = chunk.AsSpan().Slice(0, 4);
                                if (BitConverter.IsLittleEndian)
                                {
                                    contentLength.Reverse();
                                }

                                int length = BitConverter.ToInt32(contentLength);

                                yield return chunk.AsSpan().Slice(_headerSize, length).ToArray();
                            }
                            BufferPool.Instance.Return(chunk);
                        }
                        
                    }
                }
            }
        }

        public async Task WriteAsync(string key, byte[] data, DateTimeOffset appendDateTime)
        {
            string filePath = GetPersistenceFile(appendDateTime);

            _logger.LogDebug("Flushing data to disk filePath={filePath}", filePath);

            using (var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, _chunkSize, true))
            {
                byte[] bytesToSave = BufferPool.Instance.Rent(_chunkSize);

                int length = data.Length;

                // Message Length
                bytesToSave[0] = (byte)(length >> 24);
                bytesToSave[1] = (byte)(length >> 16);
                bytesToSave[2] = (byte)(length >> 8);
                bytesToSave[3] = (byte)length;

                // Set the concerned Key in the header
                // Setting length of the KEY
                byte[] dataKey = Encoding.UTF8.GetBytes(key);
                bytesToSave[4] = (byte)dataKey.Length;
                Array.Copy(dataKey, 0, bytesToSave, 5, dataKey.Length);

                // Preparing data to save
                Array.Copy(data, 0, bytesToSave, _headerSize, length);

                await fileStream.WriteAsync(bytesToSave, 0, _chunkSize);
                await fileStream.FlushAsync();

                // Add Key to index
                _memoryIndexService.TryAdd(key, new PageLocation()
                {
                    IsPersisted = true,
                    PersistenceFileLocation = filePath,
                    Position = (int)fileStream.Length - _chunkSize
                });

                BufferPool.Instance.Return(bytesToSave);
            }
        }

        private string GetPersistenceFile(DateTimeOffset appendDateTime)
        {
            string dailyFolderPath = $"{_path}/{appendDateTime.Day}";

            string hourlyFolderPath = $"{dailyFolderPath}/{appendDateTime.Hour}";

            if (!Directory.Exists(dailyFolderPath))
            {
                Directory.CreateDirectory(dailyFolderPath);
            }

            if (!Directory.Exists(hourlyFolderPath))
            {
                Directory.CreateDirectory(hourlyFolderPath);
            }

            FileInfo currentPersistenceFile = Directory
                .EnumerateFiles(hourlyFolderPath, "chunk-*.kv", SearchOption.AllDirectories)
                .Select(x => new FileInfo(x))
                .OrderByDescending(x => x.CreationTime)
                .FirstOrDefault();

            if (currentPersistenceFile == null || currentPersistenceFile.Length + _chunkSize >= _maxFileSize)
                return $"{hourlyFolderPath}/chunk-{appendDateTime.Ticks}.kv";

            return currentPersistenceFile.FullName;
        }
    }
}
