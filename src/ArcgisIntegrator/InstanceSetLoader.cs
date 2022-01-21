using ArcgisIntegrator.Config;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArcgisIntegrator;

public class InstanceSetLoader
{
    private ValidatorSettings _settings;
    private ILogger _logger;

    public InstanceSetLoader(ILogger logger, ValidatorSettings settings)
    {
        _logger = logger;
        _settings = settings;
    }

    public ChannelReader<DataEvent> Start(CancellationToken token = default)
    {
        return LoadTableData();
    }

    private ChannelReader<DataEvent> LoadTableData(CancellationToken token = default)
    {
        var instanceSetLoaderCh = Channel.CreateUnbounded<DataEvent>();

        var _ = Task.Factory.StartNew(async () =>
        {
            foreach (var tableWatch in _settings.TableWatches)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    // We share the same connection between all tables
                    // to make sure that we don't read different 'timelines'.
                    using var connection = new SqlConnection(_settings.ConnectionString);
                    await connection.OpenAsync();

                    await foreach (var sqlRow in ReadAllRows(connection, tableWatch.InitialTable))
                    {
                        var changeEvent = ChangeUtil.MapChangeEvent(new ArcgisChangeSet(sqlRow, null), tableWatch);
                        await instanceSetLoaderCh.Writer.WriteAsync(changeEvent);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Initial load channel cancelled using token.");
                    instanceSetLoaderCh.Writer.Complete();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex.StackTrace);
                }
            }

            instanceSetLoaderCh.Writer.Complete();
        });

        return instanceSetLoaderCh.Reader;
    }

    private async IAsyncEnumerable<SqlRow> ReadAllRows(SqlConnection connection, string tableName)
    {
        var sql = $"SELECT * FROM {tableName}";
        using var cmd = new SqlCommand(sql, connection);

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry"))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            yield return new SqlRow(tableName, fields);
        }

        yield break;
    }
}
