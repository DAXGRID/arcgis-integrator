using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace KonstantDataValidator.Tests;

public class ChangeUtilTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Created_operation_change_event_mapped()
    {
        var added = new List<(string fieldName, object fieldValue)>
        {
            ("OBJECTID", 10),
            ("SDE_STATE_ID", 20)
        };
        var addedSqlRow = new SqlRow("dataadmin.A524", added);
        SqlRow? deletedSqlRow = null;

        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
            "dataadmin.A524",
            added.ToDictionary(x => x.fieldName, x => x.fieldValue),
            Operation.Create);

        var result = ChangeUtil.MapChangeEvent(changeSet);
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Update_operation_change_event_mapped()
    {
        var added = new List<(string fieldName, object fieldValue)>
            {
                ("OBJECTID", 10),
                ("SDE_STATE_ID", 20)
            };
        var deleted = new List<(string fieldName, object fieldValue)>
            {
                ("OBJECTID", 10),
                ("SDE_STATE_ID", 20)
            };
        var addedSqlRow = new SqlRow("dataadmin.A524", added);
        var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
            "dataadmin.A524",
            added.ToDictionary(x => x.fieldName, x => x.fieldValue),
            Operation.Update);

        var result = ChangeUtil.MapChangeEvent(changeSet);
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Delete_operation_change_event_mapped()
    {
        var added = new List<(string fieldName, object fieldValue)>();
        var deleted = new List<(string fieldName, object fieldValue)>
        {
            ("OBJECTID", 10),
            ("SDE_STATE_ID", 20)
        };
        SqlRow? addedSqlRow = null;
        var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var expected = new ChangeEvent(
            "dataadmin.A524",
            new Dictionary<string, object>(),
            Operation.Delete);

        var result = ChangeUtil.MapChangeEvent(changeSet);
        result.Should().BeEquivalentTo(expected);
    }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void Operation_is_insert_when_only_added_table_has_changes()
    // {
    //     var added = new List<(string fieldName, object fieldValue)>
    //     {
    //         ("OBJECTID", 10),
    //         ("SDE_STATE_ID", 20)
    //     };
    //     var addedSqlRow = new SqlRow("dataadmin.A524", added);
    //     SqlRow? deletedSqlRow = null;

    //     var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

    //     var result = ChangeUtil.GetOperation(changeSet);

    //     result.Should().Be(Operation.Create);
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void Operation_is_update_both_added_and_deleted_tables_has_changes()
    // {
    //     var added = new List<(string fieldName, object fieldValue)>
    //     {
    //         ("OBJECTID", 10),
    //         ("SDE_STATE_ID", 20)
    //     };
    //     var deleted = new List<(string fieldName, object fieldValue)>
    //     {
    //         ("OBJECTID", 10),
    //         ("SDE_STATE_ID", 20)
    //     };
    //     var addedSqlRow = new SqlRow("dataadmin.A524", added);
    //     var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
    //     var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

    //     var result = ChangeUtil.GetOperation(changeSet);

    //     result.Should().Be(Operation.Update);
    // }

    // [Fact]
    // [Trait("Category", "Unit")]
    // public void Operation_is_deleted_when_only_deleted_talbe_has_changes()
    // {
    //     var deleted = new List<(string fieldName, object fieldValue)>
    //     {
    //         ("OBJECTID", 10),
    //         ("SDE_STATE_ID", 20)
    //     };
    //     SqlRow? addedSqlRow = null;
    //     var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
    //     var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

    //     var result = ChangeUtil.GetOperation(changeSet);

    //     result.Should().Be(Operation.Delete);
    // }
}
