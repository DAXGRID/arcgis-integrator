using System;
using System.Collections.Generic;

namespace ArcgisIntegrator;

internal record SqlRow
{
    public string TableName { get; init; }
    public IReadOnlyDictionary<string, object> Fields { get; init; }
    public int ObjectId =>
        (int?)Fields.GetValueOrDefault("OBJECTID") ??
        (int?)Fields.GetValueOrDefault("SDE_DELETES_ROW_ID") ??
        throw new Exception($"Could not get ObjectId from {nameof(SqlRow)}.");

    public SqlRow(string tableName, IReadOnlyDictionary<string, object> fields)
    {
        TableName = tableName;
        Fields = fields;
    }
}
