using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Linq;
using KonstantDataValidator.Config;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;

namespace KonstantDataValidator;

public class InitialChangeEventLoad
{
    private ValidatorSettings _settings;
    private ILogger _logger;

    public InitialChangeEventLoad(ILogger logger, ValidatorSettings settings)
    {
        _logger = logger;
        _settings = settings;
    }

    public ChannelReader<ChangeEvent> Start(CancellationToken token = default)
    {
        return LoadTableData();
    }

    private ChannelReader<ChangeEvent> LoadTableData(CancellationToken token = default)
    {
        var initialLoadCh = Channel.CreateUnbounded<ChangeEvent>();

        var _ = Task.Factory.StartNew(() =>
        {
            _settings.TableWatches.ToList().ForEach(async (tableWatch) =>
            {
                try
                {
                    await foreach (var sqlRow in ReadAllRows(tableWatch.InitialTable))
                    {
                        var changeEvent = ChangeUtil.MapChangeEvent(new ChangeSet(sqlRow, null), tableWatch);
                        await initialLoadCh.Writer.WriteAsync(changeEvent, token);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex.StackTrace);
                }
            });
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
