using FraudDetector.Database.Kv.Client;
using FraudDetector.Database.Kv.Engine;
using FraudDetector.Database.Kv.Engine.Disk;
using FraudDetector.Database.Kv.Engine.File;
using FraudDetector.Database.Kv.Engine.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;

namespace FraudDetector.Database.Kv.Extensions
{
    public static class KvExtensions
    {
        public static IServiceCollection AddKvStore(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddOptions();
            services.Configure<KvConfiguration>(configuration.GetSection("KvStore"));
            KvConfiguration kvConfiguration = configuration.Get<KvConfiguration>();
            if (!Directory.Exists(kvConfiguration.DbPath))
            {
                Directory.CreateDirectory(kvConfiguration.DbPath);
            }
            services.AddSingleton(typeof(IMemoryQueueProvider<>), typeof(MemoryQueueProvider<>));
            services.AddHostedService<DiskWriterQueue>();
            services.AddHostedService<MemoryIndexBuilder>();
            services.AddSingleton<DiskService>();
            services.AddSingleton<IKvEngine, KvEngine>();
            services.AddSingleton<IMemoryIndexService, MemoryIndexService>();
            services.AddSingleton<IManagedDateFileHelper, ManagedDateFileHelper>();
            services.AddTransient<IKvSet, KvSet>();
            services.AddMemoryCache();
            return services;
        }
    }
}
