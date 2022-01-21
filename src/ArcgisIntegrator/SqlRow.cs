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
        throw new ArgumentException(nameof(SqlRow), $"Could not get ObjectId.");

    public SqlRow(string tableName, IReadOnlyDictionary<string, object> fields)
    {
        TableName = tableName;
        Fields = fields;
    }
}
