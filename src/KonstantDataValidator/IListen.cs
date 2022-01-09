using System.Threading;
using System.Threading.Channels;

namespace KonstantDataValidator;

public interface IListen
{
    ChannelReader<long> Start(CancellationToken token = default);
}
