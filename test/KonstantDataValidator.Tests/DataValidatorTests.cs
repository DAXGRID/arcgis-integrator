using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MsSqlCdc;
using Xunit;
using FluentAssertions;
using FluentAssertions.Execution;
using System.Linq;

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
        var iterations = 10;
        var cTokenSource = new CancellationTokenSource();

        var sut = new Listen(_databaseFixture.ConnectionString);

        var stateIdUpdatedCh = sut.Start(cTokenSource.Token);

        var rand = new Random();
        var randNumbers = Enumerable.Range(0, iterations)
            .Select(_ => rand.Next())
            .ToArray();

        _ = Task.Factory.StartNew(() =>
        {
            for (var i = 0; i < iterations; i++)
            {
                UpdateVersionStateId(randNumbers[i], _databaseFixture.ConnectionString);
            }
        });

        var changes = new long[iterations];
        for (var i = 0; i < iterations; i++)
        {
            var change = await stateIdUpdatedCh.ReadAsync();
            changes[i] = change;
        }

        cTokenSource.Cancel();

        using (new AssertionScope())
        {
            changes.Length.Should().Be(iterations);
            changes.Should().BeEquivalentTo(randNumbers);
        }
    }

    private void UpdateVersionStateId(int stateId, string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var sql = @"UPDATE sde.SDE_versions
                    SET state_id = @state_id
                    WHERE version_id = 1;";
        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@state_id", stateId);
        cmd.ExecuteNonQuery();
    }
}
