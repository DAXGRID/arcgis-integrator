using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;
using FluentAssertions;
using FluentAssertions.Execution;
using System.Linq;
using System.Collections.Generic;

namespace KonstantDataValidator.Tests;

public class DataValidatorTests : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _databaseFixture;

    public DataValidatorTests(DatabaseFixture databaseFixture)
    {
        _databaseFixture = databaseFixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Receive_state_id_when_versions_table_row_is_updated()
    {
        var insertCount = 10;
        var cTokenSource = new CancellationTokenSource();
        var listenTables = new TableWatch[] { new TableWatch("dataadmin.KABEL", "dataadmin.A524", "dataadmin.D524") };

        var sut = new Listen(listenTables, _databaseFixture.ConnectionString);

        var stateIdUpdatedCh = sut.Start(cTokenSource.Token);

        var rand = new Random();
        var testIds = Enumerable.Range(0, insertCount)
            .Select(_ => (rand.Next(), rand.Next()))
            .ToArray() as (int objectId, int stateId)[];

        // We do this after starting 'sut.Listen' because
        // the updates are only retrieved after the listener has been started.
        _ = Task.Factory.StartNew(() =>
        {
            for (var i = 0; i < insertCount; i++)
            {
                var ids = testIds[i];
                InsertA524(ids.objectId, ids.stateId);
                UpdateVersionStateId(ids.stateId);
            }
        });

        var changes = new List<IReadOnlyCollection<ChangeEvent>>();
        for (var i = 0; i < insertCount; i++)
        {
            var change = await stateIdUpdatedCh.ReadAsync();
            changes.Add(change);
        }

        cTokenSource.Cancel();

        using (new AssertionScope())
        {
            // Should instead retrieve the updates from the tables described in the listen constructor
            changes.Count.Should().Be(insertCount);
        }
    }

    private void UpdateVersionStateId(int stateId)
    {
        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        connection.Open();
        var sql = @"UPDATE sde.SDE_versions
                    SET state_id = @state_id
                    WHERE version_id = 1;";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        cmd.ExecuteNonQuery();
    }

    private void InsertA524(int objectId, int stateId)
    {
        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        connection.Open();
        var sql = @"INSERT INTO DATA1.dataadmin.a524
                    (OBJECTID,
                     SDE_STATE_ID)
                     VALUES(@object_id, @state_id);";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        cmd.ExecuteNonQuery();
    }
}
