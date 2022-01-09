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
    private readonly int _pollingIntervalMs;
    private readonly string _versionTableName;

    public Listen(string connectionString)
    {
        _connectionString = connectionString;
        _pollingIntervalMs = 1000;
        _versionTableName = "sde_SDE_versions";
    }

    public ChannelReader<long> Start(CancellationToken token = default)
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

                    if (lowBoundLsn <= highBoundLsn)
                    {
                        var changes = await Cdc.GetAllChanges(
                            connection, _versionTableName, lowBoundLsn, highBoundLsn, AllChangesRowFilterOption.All);

                        changes
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
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    await Task.Delay(_pollingIntervalMs);
                }
            }
        });

        var stateIdUpdatedCh = Channel.CreateUnbounded<long>();
        _ = Task.Factory.StartNew(async () =>
        {
            await foreach (var versionUpdate in versionsCh.Reader.ReadAllAsync())
            {
                await stateIdUpdatedCh.Writer.WriteAsync(versionUpdate.Body.state_id);
            }

            stateIdUpdatedCh.Writer.Complete();
        });

        return stateIdUpdatedCh.Reader;
    }

    private async Task<long> GetStartLsn()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var currentMaxLsn = await Cdc.GetMaxLsn(connection);
        return await Cdc.GetNextLsn(connection, currentMaxLsn);
    }
}
