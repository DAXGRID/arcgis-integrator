using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;

namespace KonstantDataValidator.Change;

public interface IChangeEventListen
{
    ChannelReader<IReadOnlyCollection<ChangeEvent>> Start(CancellationToken token = default);
}
