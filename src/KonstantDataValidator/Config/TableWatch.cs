namespace KonstantDataValidator.Config;

public record TableWatch
{
    public string InitialTable { get; init; }
    public string AddTable { get; init; }
    public string DeleteTable { get; init; }

    public TableWatch(string initialTable, string addTable, string deleteTable)
    {
        InitialTable = initialTable;
        AddTable = addTable;
        DeleteTable = deleteTable;
    }
}
