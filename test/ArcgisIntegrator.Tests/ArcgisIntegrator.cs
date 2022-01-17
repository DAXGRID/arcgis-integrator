using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;
using FluentAssertions;
using FluentAssertions.Execution;
using System.Linq;
using System.Collections.Generic;
using ArcgisIntegrator.Config;
using FakeItEasy;
using Microsoft.Extensions.Logging;

namespace ArcgisIntegrator.Tests;

public class ArcgisIntegratorTests : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _databaseFixture;

    public ArcgisIntegratorTests(DatabaseFixture databaseFixture)
    {
        _databaseFixture = databaseFixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Receive_initial_instance_set()
    {
        // We cancel after 40 sec in case of timeouts.
        var cTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var tableWatches = new TableWatch[] { new TableWatch("dbo.cable", "dbo.a524", "dbo.D524") };
        var settings = new ValidatorSettings(_databaseFixture.ConnectionString, "sde_SDE_versions", 1000, tableWatches);
        var logger = A.Fake<ILogger>();

        // We insert 10 rows as initial dataset
        var objectIds = Enumerable.Range(0, 10).ToList();
        objectIds.ForEach(async (x) => await InsertCableTable(x));

        var sut = new InstanceSetLoader(logger, settings);
        var initialLoadCh = sut.Start();

        var result = new List<DataEvent>();
        await foreach (var changeEvent in initialLoadCh.ReadAllAsync())
        {
            result.Add(changeEvent);
        }

        using (new AssertionScope())
        {
            result.Count.Should().Be(10);
            result.Select(x => x.ObjectId).Should().BeEquivalentTo(objectIds);
            result.All(x => x.Operation == Operation.Create).Should().BeTrue();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Receive_data_events_when_versions_table_row_is_updated()
    {
        // We cancel after 40 sec in case of timeouts.
        var cTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var tableWatches = new TableWatch[] { new TableWatch("dbo.cable", "dbo.a524", "dbo.D524") };
        var settings = new ValidatorSettings(_databaseFixture.ConnectionString, "sde_SDE_versions", 1000, tableWatches);
        var logger = A.Fake<ILogger>();

        var sut = new ChangeSetListener(logger, settings);
        var stateIdUpdatedCh = sut.Start(cTokenSource.Token);

        // We do this after starting 'sut.Listen' because
        // the updates are only retrieved after the listener has been started.
        _ = Task.Factory.StartNew(async () =>
        {
            // 1. Insert
            var stateId = 1;
            await Insert524(10, stateId);
            await UpdateVersionStateId(stateId);

            // 2. Update
            stateId = 2;
            await Update524(10, stateId);
            await UpdateVersionStateId(stateId);

            // 3. Delete
            stateId = 3;
            await Delete524(10, stateId);
            await UpdateVersionStateId(stateId);

            // 4. Insert multiple
            stateId = 4;
            await Insert524(20, stateId);
            await Insert524(30, stateId);
            await UpdateVersionStateId(stateId);

            // 5. Insert, update and delete
            stateId = 5;
            await Insert524(40, stateId);
            await Update524(20, stateId);
            await Delete524(30, stateId);
            await UpdateVersionStateId(stateId);
        });

        var changes = new List<IReadOnlyCollection<DataEvent>>();
        for (var i = 0; i < 5; i++)
        {
            var change = await stateIdUpdatedCh.ReadAsync(cTokenSource.Token);
            changes.Add(change);
        }

        // We close the channels since we're done consuming.
        cTokenSource.Cancel();

        changes.Should()
            .SatisfyRespectively(
                first =>
                {
                    var firstChange = first.ToArray();
                    firstChange[0].Operation.Should().Be(Operation.Create);
                    firstChange[0].StateId.Should().Be(1);
                    firstChange[0].ObjectId.Should().Be(10);
                },
                second =>
                {
                    var secondChange = second.ToArray();
                    secondChange[0].Operation.Should().Be(Operation.Update);
                    secondChange[0].StateId.Should().Be(2);
                    secondChange[0].ObjectId.Should().Be(10);
                },
                third =>
                {
                    var thirdChange = third.ToArray();
                    thirdChange[0].Operation.Should().Be(Operation.Delete);
                    thirdChange[0].StateId.Should().Be(3);
                    thirdChange[0].ObjectId.Should().Be(10);
                },
                fourth =>
                {
                    var fourthChange = fourth.ToArray();
                    fourthChange[0].Operation.Should().Be(Operation.Create);
                    fourthChange[0].StateId.Should().Be(4);
                    fourthChange[0].ObjectId.Should().Be(20);
                    fourthChange[1].Operation.Should().Be(Operation.Create);
                    fourthChange[1].StateId.Should().Be(4);
                    fourthChange[1].ObjectId.Should().Be(30);
                },
                fifth =>
                {
                    var fifthChange = fifth.ToArray();
                    fifthChange[0].Operation.Should().Be(Operation.Create);
                    fifthChange[0].StateId.Should().Be(5);
                    fifthChange[0].ObjectId.Should().Be(40);
                    fifthChange[1].Operation.Should().Be(Operation.Update);
                    fifthChange[1].StateId.Should().Be(5);
                    fifthChange[1].ObjectId.Should().Be(20);
                    fifthChange[2].Operation.Should().Be(Operation.Delete);
                    fifthChange[2].StateId.Should().Be(5);
                    fifthChange[2].ObjectId.Should().Be(30);
                }
            );
    }

    private async Task InsertCableTable(int objectId)
    {
        var sql = @"
            INSERT INTO [dbo].[cable] ([OBJECTID])
            VALUES(@object_id)";

        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task UpdateVersionStateId(int stateId)
    {
        var sql = @"
            UPDATE [sde].[SDE_versions]
            SET [state_id] = @state_id
            WHERE [version_id] = 1";

        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task Update524(int objectId, int stateId)
    {
        await Insert524(objectId, stateId);
        await Delete524(objectId, stateId);
    }

    private async Task Insert524(int objectId, int stateId)
    {
        var sql = @"
            INSERT INTO [dbo].[a524] ([OBJECTID], [SDE_STATE_ID])
            VALUES(@object_id, @state_id)";

        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task Delete524(int objectId, int stateId)
    {
        var sql = @"
            INSERT INTO [dbo].[D524] ([SDE_DELETES_ROW_ID], [SDE_STATE_ID], [DELETED_AT])
            VALUES(@object_id, 0, @state_id)";

        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        await cmd.ExecuteNonQueryAsync();
    }
}
