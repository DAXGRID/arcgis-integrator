using ArcgisIntegrator.Config;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArcgisIntegrator;

public class InstanceSetLoader
{
    private readonly ValidatorSettings _settings;

    public InstanceSetLoader(ValidatorSettings settings)
    {
        _settings = settings;
    }

    public ChannelReader<DataEvent> Start(CancellationToken token = default) => LoadTableData(token);

    private ChannelReader<DataEvent> LoadTableData(CancellationToken token = default)
    {
        var instanceSetLoaderCh = Channel.CreateUnbounded<DataEvent>();

        _ = Task.Run(async () =>
        {
            foreach (var tableWatch in _settings.TableWatches)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    // We share the same connection between all tables
                    // to make sure that we don't read different 'timelines'.
                    using var connection = new SqlConnection(_settings.ConnectionString);
                    await connection.OpenAsync().ConfigureAwait(false);
                    await foreach (var sqlRow in ReadAllRows(connection, tableWatch.InitialTable).ConfigureAwait(false))
                    {
                        var changeEvent = ChangeUtil.MapChangeEvent(new ArcgisChangeSet(sqlRow, null), tableWatch);
                        await instanceSetLoaderCh.Writer.WriteAsync(changeEvent).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    instanceSetLoaderCh.Writer.Complete();
                }
            }

            instanceSetLoaderCh.Writer.Complete();
        }, token);

        return instanceSetLoaderCh.Reader;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage
    ("Security", "CA2100:Review SQL queries for security vulnerabilities",
     Justification = "The value is supplied doing configuration.")]
    private static async IAsyncEnumerable<SqlRow> ReadAllRows(SqlConnection connection, string tableName)
    {
        var sql = $"SELECT * FROM {tableName}";
        using var cmd = new SqlCommand(sql, connection);

        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry", StringComparison.CurrentCultureIgnoreCase))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            yield return new SqlRow(tableName, fields);
        }

        yield break;
    }
}
