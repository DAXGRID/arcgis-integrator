using System;
using System.IO;
using System.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace KonstantDataValidator.Tests.Change;

public class DatabaseFixture : IDisposable
{
    public string ConnectionString { get; init; }

    public DatabaseFixture()
    {
        ConnectionString = CreateConnectionString("DATA1");
        DeleteDatabase();
        SetupDatabase();

        // We do this because the setup process is quite intensive for the SQL database.
        // So before it can be used in tests, we want to make sure that the CDC tables are ready
        // to be consumed.
        Thread.Sleep(10000);
    }

    public void Dispose()
    {
        DeleteDatabase();
    }

    private void SetupDatabase()
    {
        using var connection = new SqlConnection(CreateConnectionString("master"));
        connection.Open();
        var setupSql = File.ReadAllText(GetRootPath("Scripts/SetupDB.sql"));
        var server = new Server(new ServerConnection(connection));
        server.ConnectionContext.ExecuteNonQuery(setupSql);
    }

    private void DeleteDatabase()
    {
        using var connection = new SqlConnection(CreateConnectionString("master"));
        connection.Open();
        var deleteDatabaseSql = @"IF DB_ID('DATA1') IS NOT NULL
                                  BEGIN
                                    ALTER DATABASE DATA1 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                                    DROP DATABASE DATA1;
                                  END;";
        using var cmd = new SqlCommand(deleteDatabaseSql, connection);
        cmd.ExecuteNonQuery();
    }

    private static string GetRootPath(string filePath)
    {
        var absolutePath = Path.IsPathRooted(filePath)
            ? filePath
            : Path.GetRelativePath(Directory.GetCurrentDirectory(), filePath);

        if (!File.Exists(absolutePath))
            throw new ArgumentException($"Could not find file at path: {absolutePath}");

        return absolutePath;
    }

    private static string CreateConnectionString(string initialCatalog)
    {
        var builder = new SqlConnectionStringBuilder();
        builder.DataSource = "localhost";
        builder.UserID = "sa";
        builder.Password = "myAwesomePassword1";
        builder.InitialCatalog = initialCatalog;
        builder.Encrypt = false;
        return builder.ConnectionString;
    }
}
