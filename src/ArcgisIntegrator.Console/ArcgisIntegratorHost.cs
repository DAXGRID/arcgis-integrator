using System.Text.Json;
using ArcgisIntegrator.Config;
using ArcgisIntegrator.Console.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArcgisIntegrator.Console;

public class ArcgisIntegratorHost : IHostedService
{
    private ILogger<ArcgisIntegratorHost> _logger;
    private CancellationTokenSource _cancellationTokenSource;
    private Settings _settings;

    public ArcgisIntegratorHost(
        ILogger<ArcgisIntegratorHost> logger,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();
        _settings = settings.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting kontant validator.");

        var tableWatches = new TableWatch[]
        {
            new TableWatch("dataadmin.KABEL", "dataadmin.a524", "dataadmin.D524")
        };

        var settings = new ValidatorSettings(
            _settings.ConnectionString, _settings.VersionTableName, 1000, tableWatches);

        Task.Factory.StartNew(async () =>
        {
            _logger.LogInformation("Starting initial load.");
            var initialLoadReaderCh = new InstanceSetLoader(_logger, settings).Start();
            await foreach (var changeEvent in initialLoadReaderCh.ReadAllAsync())
            {
                _logger.LogInformation(JsonSerializer.Serialize(changeEvent));
            }

            _logger.LogInformation("Starting listening for change events.");
            var changeEventReaderCh = new ChangeSetListener(_logger, settings).Start(_cancellationTokenSource.Token);
            await foreach (var changeEvents in changeEventReaderCh.ReadAllAsync())
            {
                _logger.LogInformation(JsonSerializer.Serialize(changeEvents));
            }
        });

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        return Task.CompletedTask;
    }
}
