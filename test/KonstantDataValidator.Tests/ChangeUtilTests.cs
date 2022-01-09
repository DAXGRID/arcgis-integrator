using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace KonstantDataValidator.Tests;

public class ChangeUtilTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Operation_is_insert_when_only_added_table_has_changes()
    {
        var added = new List<(string fieldName, object fieldValue)>
        {
            ("OBJECTID", 10),
            ("SDE_STATE_ID", 20)
        };
        var addedSqlRow = new SqlRow("dataadmin.A524", added);
        SqlRow? deletedSqlRow = null;

        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var result = ChangeUtil.GetOperation(changeSet);

        result.Should().Be(Operation.Create);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Operation_is_update_both_added_and_deleted_tables_has_changes()
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

        var result = ChangeUtil.GetOperation(changeSet);

        result.Should().Be(Operation.Update);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Operation_is_deleted_when_only_deleted_talbe_has_changes()
    {
        var deleted = new List<(string fieldName, object fieldValue)>
        {
            ("OBJECTID", 10),
            ("SDE_STATE_ID", 20)
        };
        SqlRow? addedSqlRow = null;
        var deletedSqlRow = new SqlRow("dataadmin.D524", deleted);
        var changeSet = new ChangeSet(addedSqlRow, deletedSqlRow);

        var result = ChangeUtil.GetOperation(changeSet);

        result.Should().Be(Operation.Delete);
    }
}
