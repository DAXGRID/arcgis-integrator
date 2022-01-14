namespace ArcgisIntegrator.Console.Config;

public record Settings
{
    public string ConnectionString { get; init; } = "";
    public string VersionTableName { get; init; } = "";
    public int PollingIntervalMs { get; init; }
}
