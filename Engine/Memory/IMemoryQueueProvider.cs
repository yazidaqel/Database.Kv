using System.Threading.Channels;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    internal interface IMemoryQueueProvider<T> where T : class
    {
        ChannelReader<T> GetChannelReader();
        ChannelWriter<T> GetChannelWriter();
    }
}
