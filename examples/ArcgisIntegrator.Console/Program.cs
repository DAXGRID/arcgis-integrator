using ArcgisIntegrator.Config;
using Microsoft.Extensions.Hosting;

namespace ArcgisIntegrator.Console;

public class Program
{
    static async Task Main(string[] args)
    {
        using (var host = HostConfig.Configure())
        {
            await host.StartAsync();
            await host.WaitForShutdownAsync();
        }
    }
}
