using System;
using System.Collections.Generic;

namespace ArcgisIntegrator.Config;

public record ValidatorSettings
{
    public string ConnectionString { get; init; }
    public string VersionTableName { get; init; }
    public int PollingIntervalMs { get; init; }
    public IReadOnlyCollection<TableWatch> TableWatches { get; init; }

    public ValidatorSettings(
        string connectionString,
        string versionsTableName,
        int pollingIntervalMs,
        IReadOnlyCollection<TableWatch> tableWatches)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("ConnectionString cannot be null, empty or whitespace.");
        if (string.IsNullOrWhiteSpace(versionsTableName))
            throw new ArgumentException("Version table name cannot be null, empty or whitespace.");
        if (tableWatches is null)
            throw new ArgumentException("Table watches cannot be null");
        if (tableWatches.Count == 0)
            throw new ArgumentException("Table watches count 0.");

        ConnectionString = connectionString;
        VersionTableName = versionsTableName;
        PollingIntervalMs = pollingIntervalMs;
        TableWatches = tableWatches;
    }
}
