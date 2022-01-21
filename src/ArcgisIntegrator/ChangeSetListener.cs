using ArcgisIntegrator.Config;
using Microsoft.Data.SqlClient;
using MsSqlCdc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArcgisIntegrator;

public class ChangeSetListener
{
    private readonly ValidatorSettings _settings;

    public ChangeSetListener(ValidatorSettings settings)
    {
        _settings = settings;
    }

    public ChannelReader<IReadOnlyCollection<DataEvent>> Start(CancellationToken token = default)
    {
        var versionChangeCh = VersionChangeCh(token);
        var changeEventCh = ChangeEventCh(versionChangeCh);
        return changeEventCh;
    }

    private ChannelReader<AllChangeRow> VersionChangeCh(CancellationToken token)
    {
        var versionChangeCh = Channel.CreateUnbounded<AllChangeRow>();
        _ = Task.Run(async () =>
        {
            var lowBoundLsn = new BigInteger(-1);
            while (true)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    if (lowBoundLsn == -1)
                        lowBoundLsn = await GetStartLsn().ConfigureAwait(false);

                    using var connection = new SqlConnection(_settings.ConnectionString);
                    await connection.OpenAsync().ConfigureAwait(false);

                    var highBoundLsn = await Cdc.GetMaxLsn(connection).ConfigureAwait(false);

                    // If the highbound lsn has changed there might be a change to the table.
                    if (lowBoundLsn <= highBoundLsn)
                    {
                        var changes = await Cdc.GetAllChanges(
                            connection,
                            _settings.VersionTableName,
                            lowBoundLsn,
                            highBoundLsn,
                            AllChangesRowFilterOption.All).ConfigureAwait(false);

                        foreach (var change in changes.OrderBy(x => x.SequenceValue))
                            await versionChangeCh.Writer.WriteAsync(change).ConfigureAwait(false);

                        lowBoundLsn = await Cdc.GetNextLsn(connection, highBoundLsn).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    versionChangeCh.Writer.Complete();
                    break;
                }
                finally
                {
                    await Task.Delay(_settings.PollingIntervalMs).ConfigureAwait(false);
                }
            }
        }, token).ConfigureAwait(false);

        return versionChangeCh.Reader;
    }

    private ChannelReader<IReadOnlyCollection<DataEvent>> ChangeEventCh(ChannelReader<AllChangeRow> versionsCh)
    {
        var changeEventCh = Channel.CreateUnbounded<IReadOnlyCollection<DataEvent>>();
        _ = Task.Run(async () =>
        {
            await foreach (var versionUpdate in versionsCh.ReadAllAsync())
            {
                var stateId = (long)versionUpdate.Fields["state_id"];
                var changeEvents = new List<DataEvent>();
                foreach (var table in _settings.TableWatches)
                {
                    var addedTask = RetrieveRowAdd(table.AddTable, stateId);
                    var deletedTask = RetrieveRowDelete(table.DeleteTable, stateId);

                    var changes = (await Task.WhenAll(addedTask, deletedTask).ConfigureAwait(false))
                        .SelectMany(x => x)
                        .GroupBy(x => x.ObjectId)
                        .Select(x => new ArcgisChangeSet(
                                    x.FirstOrDefault(y => y.TableName == table.AddTable),
                                    x.FirstOrDefault(y => y.TableName == table.DeleteTable)))
                        .Select(x => ChangeUtil.MapChangeEvent(x, table));

                    changeEvents.AddRange(changes);
                }

                await changeEventCh.Writer.WriteAsync(changeEvents).ConfigureAwait(false);
            }

            changeEventCh.Writer.Complete();
        });

        return changeEventCh;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage
    ("Security", "CA2100:Review SQL queries for security vulnerabilities",
     Justification = "The value is supplied doing configuration.")]
    private async Task<List<SqlRow>> RetrieveRowAdd(string tableName, long stateId)
    {
        var sql = $@"SELECT *
                     FROM {tableName}
                     WHERE SDE_STATE_ID = @state_id";

        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new SqlCommand(sql, connection);
        _ = cmd.Parameters.AddWithValue("@state_id", stateId);

        var sqlRowList = new List<SqlRow>();
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry", StringComparison.OrdinalIgnoreCase))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            sqlRowList.Add(new SqlRow(tableName, fields));
        }

        return sqlRowList;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage
    ("Security", "CA2100:Review SQL queries for security vulnerabilities",
     Justification = "The value is supplied doing configuration.")]
    private async Task<List<SqlRow>> RetrieveRowDelete(string tableName, long stateId)
    {
        var sql = $@"SELECT *
                     FROM {tableName}
                     WHERE SDE_STATE_ID = 0 AND DELETED_AT = @state_id";

        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        using var cmd = new SqlCommand(sql, connection);
        _ = cmd.Parameters.AddWithValue("@state_id", stateId);

        var sqlRowList = new List<SqlRow>();
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var fields = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry", StringComparison.OrdinalIgnoreCase))
                    fields.Add(reader.GetName(i), reader.GetValue(i));
            }

            sqlRowList.Add(new SqlRow(tableName, fields));
        }

        return sqlRowList;
    }

    private async Task<BigInteger> GetStartLsn()
    {
        using var connection = new SqlConnection(_settings.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        var currentMaxLsn = await Cdc.GetMaxLsn(connection).ConfigureAwait(false);
        return await Cdc.GetNextLsn(connection, currentMaxLsn).ConfigureAwait(false);
    }
}
