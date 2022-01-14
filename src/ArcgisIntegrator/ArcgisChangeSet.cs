namespace ArcgisIntegrator;

internal record ArcgisChangeSet
{
    public SqlRow? Add { get; init; }
    public SqlRow? Delete { get; init; }

    public ArcgisChangeSet(SqlRow? add, SqlRow? delete)
    {
        Add = add;
        Delete = delete;
    }
}
