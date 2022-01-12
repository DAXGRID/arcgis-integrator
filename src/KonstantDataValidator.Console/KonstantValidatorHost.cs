using System.Text.Json;
using KonstantDataValidator.Change;
using KonstantDataValidator.Console.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KonstantDataValidator.Console;

public class KonstantValidatorHost : IHostedService
{
    private ILogger<KonstantValidatorHost> _logger;
    private CancellationTokenSource _cancellationTokenSource;
    private Settings _settings;

    public KonstantValidatorHost(
        ILogger<KonstantValidatorHost> logger,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();
        _settings = settings.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogError("TEST" + _settings.ConnectionString);

        _logger.LogInformation("Starting kontant validator.");

        var tableWatches = new TableWatch[]
        {
            new TableWatch("dataadmin.KABEL", "dataadmin.a524", "dataadmin.D524")
        };

        var settings = new KonstantDataValidator.Config.Settings(
            _settings.ConnectionString, _settings.VersionTableName, 1000, tableWatches);

        var changeEventListen = new ChangeEventListen(_logger, settings);
        var changeEventReader = changeEventListen.Start(_cancellationTokenSource.Token);

        Task.Factory.StartNew(async () =>
        {
            await foreach (var changeEvents in changeEventReader.ReadAllAsync())
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
