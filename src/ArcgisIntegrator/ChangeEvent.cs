using System.Collections.Generic;
using ArcgisIntegrator.Config;

namespace ArcgisIntegrator;

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

    /// <summary>
    /// When operation is delete we get the value from 'DELETED_AT',
    /// Otherwise we get it from 'SDE_STATE_ID' in case of initial load the value might be -1.
    /// </summary>
    public long StateId => Operation == Operation.Delete
        ? (long)Fields["DELETED_AT"]
        : ((long?)Fields.GetValueOrDefault("SDE_STATE_ID") ?? -1);

    public ChangeEvent(TableWatch tables, IReadOnlyDictionary<string, object> columns, Operation operation)
    {
        Tables = tables;
        Fields = columns;
        Operation = operation;
    }
}
