using System.Collections.Generic;
using KonstantDataValidator.Config;

namespace KonstantDataValidator;

public enum Operation
{
    Create,
    Update,
    Delete
}

public record ChangeEvent
{
    public TableWatch Tables { get; init; }
    public IReadOnlyDictionary<string, object> Fields { get; init; }
    public Operation Operation { get; init; }
    public int ObjectId => (int)(Operation == Operation.Delete
                                 ? Fields["SDE_DELETES_ROW_ID"]
                                 : Fields["OBJECTID"]);
    public long StateId => (long)(Operation == Operation.Delete
                                  ? Fields["DELETED_AT"]
                                  : Fields["SDE_STATE_ID"]);

    public ChangeEvent(TableWatch tables, IReadOnlyDictionary<string, object> columns, Operation operation)
    {
        Tables = tables;
        Fields = columns;
        Operation = operation;
    }
}