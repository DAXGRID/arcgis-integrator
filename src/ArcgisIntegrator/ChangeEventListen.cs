using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using MsSqlCdc;
using Microsoft.Extensions.Logging;
using ArcgisIntegrator.Config;
using System.Numerics;

namespace ArcgisIntegrator;

public class ChangeEventListen
{
    private readonly ILogger _logger;
    private readonly ValidatorSettings _settings;

    public ChangeEventListen(
        ILogger logger,
        ValidatorSettings settings)
    {
        _logger = logger;
        _settings = settings;
    }

    public ChannelReader<IReadOnlyCollection<ChangeEvent>> Start(CancellationToken token = default)
    {
        var versionChangeCh = VersionChangeCh(token);
        var changeEventCh = ChangeEventCh(versionChangeCh);
        return changeEventCh;
    }

    private ChannelReader<ChangeRow> VersionChangeCh(CancellationToken token)
    {
        var versionChangeCh = Channel.CreateUnbounded<ChangeRow>();
        _ = Task.Factory.StartNew<Task>(async () =>
        {
            var lowBoundLsn = new BigInteger(-1);
            while (true)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    if (lowBoundLsn == -1)
                        lowBoundLsn = await GetStartLsn();

                    using var connection = new SqlConnection(_settings.ConnectionString);
                    await connection.OpenAsync();

                    var highBoundLsn = await Cdc.GetMaxLsn(connection);

                    // If the highbound lsn has changed there might be a change to the table.
                    if (lowBoundLsn <= highBoundLsn)
                    {
                        (await Cdc.GetAllChanges(connection,
                                                 _settings.VersionTableName,
                                                 lowBoundLsn,
                                                 highBoundLsn,
                                                 AllChangesRowFilterOption.All))
                            .OrderBy(x => x.SequenceValue)
                            .ToList()
                            .ForEach(async (x) => await versionChangeCh.Writer.WriteAsync(x));

                        lowBoundLsn = await Cdc.GetNextLsn(connection, highBoundLsn);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("ChangeRow channel cancelled using token.");
                    versionChangeCh.Writer.Complete();
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex);
                }
                finally
                {
                    await Task.Delay(_settings.PollingIntervalMs);
                }
            }
        });

        return versionChangeCh.Reader;
    }

    private ChannelReader<IReadOnlyCollection<ChangeEvent>> ChangeEventCh(
        ChannelReader<ChangeRow> versionsCh)
    {
        var changeEventCh = Channel.CreateUnbounded<IReadOnlyCollection<ChangeEvent>>();
        _ = Task.Factory.StartNew<Task>(async () =>
        {
            await foreach (var versionUpdate in versionsCh.ReadAllAsync())
            {
                try
                {
                    var stateId = (long)versionUpdate.Fields["state_id"];
                    var changeEvents = new List<ChangeEvent>();
                    foreach (var table in _settings.TableWatches)
                    {
                        var addedTask = RetrieveRowAdd(table.AddTable, stateId);
                        var deletedTask = RetrieveRowDelete(table.DeleteTable, stateId);

                        var changes = (await Task.WhenAll(addedTask, deletedTask))
                            .SelectMany(x => x)
                            .GroupBy(x => x.ObjectId)
                            .Select(x => new ChangeSet(
                                        x.FirstOrDefault(y => y.TableName == table.AddTable),
                                        x.FirstOrDefault(y => y.TableName == table.DeleteTable)))
                            .Select(x => ChangeUtil.MapChangeEvent(x, table));

                        changeEvents.AddRange(changes);
                    }

                    await changeEventCh.Writer.WriteAsync(changeEvents);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex);
                }
            }

            changeEventCh.Writer.Complete();
        });

        return changeEventCh;
    }

    private async Task<List<SqlRow>> RetrieveRowAdd(string tableName, long stateId)
    {
        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync();

        var sql = $@"SELECT *
                     FROM {tableName}
                     WHERE SDE_STATE_ID = @state_id";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);

        var sqlRowList = new List<SqlRow>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry"))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            sqlRowList.Add(new SqlRow(tableName, fields));
        }

        return sqlRowList;
    }

    private async Task<List<SqlRow>> RetrieveRowDelete(string tableName, long stateId)
    {
        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync();

        var sql = $@"SELECT *
                     FROM {tableName}
                     WHERE SDE_STATE_ID = 0 AND DELETED_AT = @state_id";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);

        var sqlRowList = new List<SqlRow>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry"))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            sqlRowList.Add(new SqlRow(tableName, fields));
        }

        return sqlRowList;
    }

    private async Task<BigInteger> GetStartLsn()
    {
        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync();
        var currentMaxLsn = await Cdc.GetMaxLsn(connection);
        return await Cdc.GetNextLsn(connection, currentMaxLsn);
    }
}
