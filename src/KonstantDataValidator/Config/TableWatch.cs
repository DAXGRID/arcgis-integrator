namespace KonstantDataValidator.Config;

public record TableWatch
{
    public string Table { get; init; }
    public string AddTable { get; init; }
    public string DeleteTable { get; init; }

    public TableWatch(string table, string addTable, string deleteTable)
    {
        Table = table;
        AddTable = addTable;
        DeleteTable = deleteTable;
    }
}
