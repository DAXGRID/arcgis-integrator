using System.Threading;
using System.Threading.Channels;
using MsSqlCdc;

namespace KonstantDataValidator;

public interface IListen
{
    Channel<ChangeRow<dynamic>> Start(CancellationToken token = default);
}
