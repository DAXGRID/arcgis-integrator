using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using ArcgisIntegrator.Config;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;

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
        var initialLoadCh = Channel.CreateUnbounded<DataEvent>();

        var _ = Task.Factory.StartNew(async () =>
        {
            foreach (var tableWatch in _settings.TableWatches)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    await foreach (var sqlRow in ReadAllRows(tableWatch.InitialTable))
                    {
                        var changeEvent = ChangeUtil.MapChangeEvent(new ArcgisChangeSet(sqlRow, null), tableWatch);
                        await initialLoadCh.Writer.WriteAsync(changeEvent);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Initial load channel cancelled using token.");
                    initialLoadCh.Writer.Complete();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex.StackTrace);
                }
            }

            initialLoadCh.Writer.Complete();
        });

        return initialLoadCh.Reader;
    }

    private async IAsyncEnumerable<SqlRow> ReadAllRows(string tableName)
    {
        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync();

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
