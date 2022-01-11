using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using MsSqlCdc;

namespace KonstantDataValidator;

public record ChangeSet
{
    public SqlRow? Add { get; init; }
    public SqlRow? Delete { get; init; }
    public string TableName =>
        Add?.TableName ?? Delete?.TableName ?? throw new Exception("Could not find table name.");

    public ChangeSet(SqlRow? add = null, SqlRow? delete = null)
    {
        Add = add;
        Delete = delete;
    }
}

public record SqlRow
{
    public string TableName { get; init; }
    public IReadOnlyCollection<(string fieldName, object fieldValue)> Fields;
    public int ObjectId =>
        (int)Fields.First(x => x.fieldName == "OBJECTID" || x.fieldName == "SDE_DELETES_ROW_ID").fieldValue;

    public SqlRow(string tableName, IReadOnlyCollection<(string fieldName, object fieldValue)> fields)
    {
        TableName = tableName;
        Fields = fields;
    }
}

public record TableWatch
{
    public string Table { get; init; }
    public string AddTable { get; init; }
    public string DeleteTable { get; init; }

    public TableWatch(string table, string addTable, string deleteTable)
    {
        Table = table;
        AddTable = addTable;
        DeleteTable = deleteTable;
    }
}

public class Listen : IListen
{
    private readonly string _connectionString;
    private readonly int _pollingIntervalMs;
    private readonly string _versionTableName;
    private readonly IReadOnlyCollection<TableWatch> _tables;

    public Listen(IReadOnlyCollection<TableWatch> tables, string connectionString)
    {
        _connectionString = connectionString;
        _pollingIntervalMs = 1000; // TODO get from constructor
        _versionTableName = "sde_SDE_versions"; // TODO get from constructor
        _tables = tables;
    }

    public ChannelReader<IReadOnlyCollection<ChangeEvent>> Start(CancellationToken token = default)
    {
        var versionsCh = Channel.CreateUnbounded<ChangeRow<dynamic>>();

        _ = Task.Factory.StartNew(async () =>
        {
            var lowBoundLsn = -1L;
            while (true)
            {
                try
                {
                    token.ThrowIfCancellationRequested();

                    if (lowBoundLsn == -1)
                        lowBoundLsn = await GetStartLsn();

                    using var connection = new SqlConnection(_connectionString);
                    await connection.OpenAsync();

                    var highBoundLsn = await Cdc.GetMaxLsn(connection);

                    // If the highbound lsn has changed there might be a change to the table.
                    if (lowBoundLsn <= highBoundLsn)
                    {
                        (await Cdc.GetAllChanges(
                            connection, _versionTableName, lowBoundLsn, highBoundLsn, AllChangesRowFilterOption.All))
                            .OrderBy(x => x.SequenceValue)
                            .ToList()
                            .ForEach(async (x) => await versionsCh.Writer.WriteAsync(x));

                        lowBoundLsn = await Cdc.GetNextLsn(connection, highBoundLsn);
                    }
                }
                catch (OperationCanceledException)
                {
                    versionsCh.Writer.Complete();
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message); // TODO use logging
                }
                finally
                {
                    await Task.Delay(_pollingIntervalMs);
                }
            }
        });

        var updateRowCh = Channel.CreateUnbounded<IReadOnlyCollection<ChangeEvent>>();
        _ = Task.Factory.StartNew(async () =>
        {
            await foreach (var versionUpdate in versionsCh.Reader.ReadAllAsync())
            {
                try
                {
                    long stateId = versionUpdate.Body.state_id;
                    var changeEvents = new List<ChangeEvent>();
                    foreach (var table in _tables)
                    {
                        var addedTask = RetrieveRowUpdate(table.AddTable, stateId);
                        var deletedTask = RetrieveRowUpdate(table.DeleteTable, stateId);

                        var changes = (await Task.WhenAll(addedTask, deletedTask))
                            .SelectMany(x => x)
                            .GroupBy(x => x.ObjectId)
                            .Select(x => new ChangeSet(
                                        x.FirstOrDefault(y => y.TableName == table.AddTable),
                                        x.FirstOrDefault(y => y.TableName == table.DeleteTable)))
                            .Select(x => ChangeUtil.MapChangeEvent(x, table));

                        changeEvents.AddRange(changes);
                    }

                    await updateRowCh.Writer.WriteAsync(changeEvents);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message); // TODO use logging
                }
            }

            updateRowCh.Writer.Complete();
        });

        return updateRowCh.Reader;
    }

    private async Task<List<SqlRow>> RetrieveRowUpdate(string tableName, long stateId)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $@"SELECT * FROM {tableName}
                     WHERE SDE_STATE_ID = @state_id";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);

        var sqlRowList = new List<SqlRow>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var column = new List<(string fieldName, object fieldValue)>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (!reader.GetDataTypeName(i).Contains("geometry"))
                {
                    column.Add((reader.GetName(i), reader.GetValue(i)));
                }
            }

            sqlRowList.Add(new SqlRow(tableName, column));
        }

        return sqlRowList;
    }

    private async Task<long> GetStartLsn()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var currentMaxLsn = await Cdc.GetMaxLsn(connection);
        return await Cdc.GetNextLsn(connection, currentMaxLsn);
    }
}
