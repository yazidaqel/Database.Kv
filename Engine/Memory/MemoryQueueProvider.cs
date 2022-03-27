using System.Threading.Channels;

namespace FraudDetector.Database.Kv.Engine.Memory
{
    internal class MemoryQueueProvider<T> : IMemoryQueueProvider<T> where T : class
    {
        private readonly ChannelReader<T> _reader;
        private readonly ChannelWriter<T> _writer;
        public MemoryQueueProvider()
        {
            Channel<T> diskWriteChannel = Channel.CreateBounded<T>(
             new BoundedChannelOptions(1024)
             {
                 FullMode = BoundedChannelFullMode.Wait,
                 SingleReader = true,
                 SingleWriter = true
             });

            _writer = diskWriteChannel.Writer;
            _reader = diskWriteChannel.Reader;
        }

        public ChannelReader<T> GetChannelReader()
        {
            return _reader;
        }

        public ChannelWriter<T> GetChannelWriter()
        {
            return _writer;
        }
    }
}
