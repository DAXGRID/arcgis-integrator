using ArcgisIntegrator.Config;
using ArcgisIntegrator.Console.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace ArcgisIntegrator.Console;

public class ArcgisIntegratorHost : BackgroundService
{
    private readonly ILogger<ArcgisIntegratorHost> _logger;
    private readonly Settings _settings;

    public ArcgisIntegratorHost(
        ILogger<ArcgisIntegratorHost> logger,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
    }

    protected async override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting kontant validator.");

        var tableWatches = new TableWatch[]
        {
            new TableWatch("dbo.cable", "dbo.a524", "dbo.D524")
        };

        var settings = new ValidatorSettings(
            _settings.ConnectionString, _settings.VersionTableName, 1000, tableWatches);

        try
        {
            _logger.LogInformation("Starting initial load.");
            var instanceSetLoaderReaderCh = new InstanceSetLoader(settings).Start();
            await foreach (var changeEvent in instanceSetLoaderReaderCh.ReadAllAsync(cancellationToken))
            {
                _logger.LogInformation(JsonSerializer.Serialize(changeEvent));
            }

            _logger.LogInformation("Starting listening for change events.");
            var changeSetListenerCh = new ChangeSetListener(settings).Start(cancellationToken);
            await foreach (var changeEvents in changeSetListenerCh.ReadAllAsync(cancellationToken))
            {
                _logger.LogInformation(JsonSerializer.Serialize(changeEvents));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
            throw;
        }
    }
}
