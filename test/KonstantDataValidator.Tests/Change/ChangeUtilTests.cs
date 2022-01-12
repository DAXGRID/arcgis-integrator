using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using KonstantDataValidator.Change;
using Xunit;

namespace KonstantDataValidator.Tests.Change;

public class ChangeUtilTests
{
    private TableWatch CreateTableWatchDefault()
    {
        return new TableWatch("dataadmin.KABEL", "dataadmin.a524", "dataadmin.D524");
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
        var addedSqlRow = new SqlRow("dataadmin.a524", added);
        SqlRow? deletedSqlRow = null;

        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
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
        var addedSqlRow = new SqlRow("dataadmin.a524", added);
        var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
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
            {"SDE_STATE_ID", 20L}
        };
        SqlRow? addedSqlRow = null;
        var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
            tables,
            deleted,
            Operation.Delete);

        var result = ChangeUtil.MapChangeEvent(changeSet, tables);
        result.Should().BeEquivalentTo(expected);
    }
}
