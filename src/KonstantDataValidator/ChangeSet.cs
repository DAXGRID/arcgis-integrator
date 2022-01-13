namespace KonstantDataValidator;

internal record ChangeSet
{
    public SqlRow? Add { get; init; }
    public SqlRow? Delete { get; init; }

    public ChangeSet(SqlRow? add, SqlRow? delete)
    {
        Add = add;
        Delete = delete;
    }
}
