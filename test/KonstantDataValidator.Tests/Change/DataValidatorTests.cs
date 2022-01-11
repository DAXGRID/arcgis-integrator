using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;
using FluentAssertions;
using FluentAssertions.Execution;
using System.Linq;
using System.Collections.Generic;
using KonstantDataValidator.Change;

namespace KonstantDataValidator.Tests.Change;

public class DataValidatorTests : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _databaseFixture;

    public DataValidatorTests(DatabaseFixture databaseFixture)
    {
        _databaseFixture = databaseFixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Receive_change_event_when_versions_table_row_is_updated()
    {
        // We cancel after 40 sec in case of timeouts.
        var cTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var listenTables = new TableWatch[] { new TableWatch("dataadmin.KABEL", "dataadmin.a524", "dataadmin.D524") };

        var sut = new ChangeEventListen(listenTables, _databaseFixture.ConnectionString);

        var stateIdUpdatedCh = sut.Start(cTokenSource.Token);

        // We do this after starting 'sut.Listen' because
        // the updates are only retrieved after the listener has been started.
        _ = Task.Factory.StartNew(() =>
        {
            // 1. Insert
            var stateId = 1;
            InsertA524(10, stateId);
            UpdateVersionStateId(stateId);

            // 2. Update
            stateId = 2;
            InsertA524(10, stateId);
            DeleteD524(10, stateId);
            UpdateVersionStateId(stateId);

            // 3. Delete
            stateId = 3;
            DeleteD524(10, stateId);
            UpdateVersionStateId(stateId);

            // 4. Insert multiple
            stateId = 4;
            InsertA524(20, stateId);
            InsertA524(30, stateId);
            UpdateVersionStateId(stateId);

            // 5. Insert, update and delete
            stateId = 5;
            InsertA524(40, stateId);
            InsertA524(20, stateId);
            DeleteD524(20, stateId);
            DeleteD524(30, stateId);
            UpdateVersionStateId(stateId);
        });

        var changes = new List<IReadOnlyCollection<ChangeEvent>>();
        for (var i = 0; i < 5; i++)
        {
            var change = await stateIdUpdatedCh.ReadAsync();
            changes.Add(change);
        }

        cTokenSource.Cancel();

        using (new AssertionScope())
        {
            // 1. Insert
            var firstChange = changes[0].ToArray();
            firstChange[0].Operation.Should().Be(Operation.Create);
            firstChange[0].StateId.Should().Be(1);
            firstChange[0].ObjectId.Should().Be(10);

            // 2. Update
            var secondChange = changes[1].ToArray();
            secondChange[0].Operation.Should().Be(Operation.Update);
            secondChange[0].StateId.Should().Be(2);
            secondChange[0].ObjectId.Should().Be(10);

            // 3. Delete
            var thirdChange = changes[2].ToArray();
            thirdChange[0].Operation.Should().Be(Operation.Delete);
            thirdChange[0].StateId.Should().Be(3);
            thirdChange[0].ObjectId.Should().Be(10);

            // 4. Insert multiple
            var fourthChange = changes[3].ToArray();
            fourthChange[0].Operation.Should().Be(Operation.Create);
            fourthChange[0].StateId.Should().Be(4);
            fourthChange[0].ObjectId.Should().Be(20);
            fourthChange[1].Operation.Should().Be(Operation.Create);
            fourthChange[1].StateId.Should().Be(4);
            fourthChange[1].ObjectId.Should().Be(30);

            // 5. Insert, update and delete
            var fifthChange = changes[4].ToArray();
            fifthChange[0].Operation.Should().Be(Operation.Update);
            fifthChange[0].StateId.Should().Be(5);
            fifthChange[0].ObjectId.Should().Be(20);
            fifthChange[1].Operation.Should().Be(Operation.Create);
            fifthChange[1].StateId.Should().Be(5);
            fifthChange[1].ObjectId.Should().Be(40);
            fifthChange[2].Operation.Should().Be(Operation.Delete);
            fifthChange[2].StateId.Should().Be(5);
            fifthChange[2].ObjectId.Should().Be(30);
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
        var sql = @"INSERT INTO dataadmin.a524
                    (OBJECTID,
                     SDE_STATE_ID)
                     VALUES(@object_id, @state_id);";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        cmd.ExecuteNonQuery();
    }

    private void DeleteD524(int objectId, int stateId)
    {
        using var connection = new SqlConnection(_databaseFixture.ConnectionString);
        connection.Open();
        var sql = @"INSERT INTO dataadmin.D524
                    (SDE_DELETES_ROW_ID,
                     SDE_STATE_ID,
                     DELETED_AT)
                     VALUES(@object_id, @state_id, 0);";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@object_id", objectId);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        cmd.ExecuteNonQuery();
    }
}
