using ArcgisIntegrator.Config;
using FluentAssertions;
using System.Collections.Generic;
using Xunit;

namespace ArcgisIntegrator.Tests;

public class ChangeUtilTests
{
    private TableWatch CreateTableWatchDefault()
    {
        return new TableWatch("dbo.cable", "dbo.a524", "dbo.D524");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Created_operation_change_event_mapped()
    {
        var tables = CreateTableWatchDefault();
        var added = new Dictionary<string, object>
        {
            {"OBJECTID", 10},
            {"SDE_STATE_ID", 20L}
        };
        var addedSqlRow = new SqlRow(tables.AddTable, added);
        SqlRow? deletedSqlRow = null;

        var changeSet = new ArcgisChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new DataEvent(
            tables,
            added,
            Operation.Create);

        var result = ChangeUtil.MapChangeEvent(changeSet, tables);
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Update_operation_change_event_mapped()
    {
        var tables = CreateTableWatchDefault();
        var added = new Dictionary<string, object>
        {
            {"OBJECTID", 10},
            {"SDE_STATE_ID", 20L}
        };
        var deleted = new Dictionary<string, object>
        {
            {"OBJECTID", 10},
            {"SDE_STATE_ID", 20L}
        };
        var addedSqlRow = new SqlRow(tables.AddTable, added);
        var deletedSqlRow = new SqlRow(tables.DeleteTable, deleted);
        var changeSet = new ArcgisChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new DataEvent(
            tables,
            added,
            Operation.Update);

        var result = ChangeUtil.MapChangeEvent(changeSet, tables);
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Delete_operation_change_event_mapped()
    {
        var tables = CreateTableWatchDefault();
        var added = new List<(string fieldName, object fieldValue)>();
        var deleted = new Dictionary<string, object>
        {
            {"SDE_DELETES_ROW_ID", 10},
            {"DELETED_AT", 0L}
        };
        SqlRow? addedSqlRow = null;
        var deletedSqlRow = new SqlRow(tables.DeleteTable, deleted);
        var changeSet = new ArcgisChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new DataEvent(
            tables,
            deleted,
            Operation.Delete);

        var result = ChangeUtil.MapChangeEvent(changeSet, tables);
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Initial_sql_row_mapped_as_created()
    {
        var tables = CreateTableWatchDefault();
        var added = new Dictionary<string, object>
        {
            {"OBJECTID", 10}
        };
        var addedSqlRow = new SqlRow(tables.InitialTable, added);
        SqlRow? deletedSqlRow = null;
        var changeSet = new ArcgisChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new DataEvent(
            tables,
            added,
            Operation.Create);

        var result = ChangeUtil.MapChangeEvent(changeSet, tables);
        result.Should().BeEquivalentTo(expected);
    }
}
