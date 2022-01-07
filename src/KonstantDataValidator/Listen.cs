using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MsSqlCdc;

namespace KonstantDataValidator;

public class Listen : IListen
{
    private readonly string _connectionString;
    private int _pollingIntervalMs;

    public Listen(string connectionString)
    {
        _connectionString = connectionString;
        _pollingIntervalMs = 1000;
    }

    public Channel<ChangeRow<dynamic>> Start(CancellationToken token = default)
    {
        var changeDataCh = Channel.CreateUnbounded<ChangeRow<dynamic>>();
        var versionsTable = "sde_SDE_versions";

        _ = Task.Factory.StartNew(async () =>
        {
            var lowBoundLsn = -1L;
            while (true)
            {
                try
                {
                    if (lowBoundLsn == -1)
                        lowBoundLsn = await GetStartLsn();

                    token.ThrowIfCancellationRequested();
                    using var connection = new SqlConnection(_connectionString);
                    await connection.OpenAsync();

                    var highBoundLsn = await Cdc.GetMaxLsn(connection);

                    if (lowBoundLsn <= highBoundLsn)
                    {
                        var changes = await Cdc.GetAllChanges(
                            connection, versionsTable, lowBoundLsn, highBoundLsn, AllChangesRowFilterOption.All);

                        changes.OrderBy(x => x.SequenceValue)
                            .ToList()
                            .ForEach(async (x) => await changeDataCh.Writer.WriteAsync(x, token));

                        lowBoundLsn = await Cdc.GetNextLsn(connection, highBoundLsn);
                    }
                }
                catch (OperationCanceledException)
                {
                    changeDataCh.Writer.Complete();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    await Task.Delay(_pollingIntervalMs);
                }
            }
        }, token);

        return changeDataCh;
    }

    private async Task<long> GetStartLsn()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var currentMaxLsn = await Cdc.GetMaxLsn(connection);
        return await Cdc.GetNextLsn(connection, currentMaxLsn);
    }
}
