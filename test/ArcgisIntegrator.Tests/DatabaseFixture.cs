using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.IO;
using System.Threading;

namespace ArcgisIntegrator.Tests;

internal class DatabaseFixture
{
    public static string ConnectionString => CreateConnectionString("SUPERGIS");

    public DatabaseFixture()
    {
        DeleteDatabase();
        SetupDatabase();

        // We do this because the setup process is quite intensive for the SQL database.
        // So before it can be used in tests, we want to make sure that the CDC tables are ready to be consumed.
        Thread.Sleep(2000);
    }

    private static void SetupDatabase()
    {
        using var connection = new SqlConnection(CreateConnectionString("master"));
        connection.Open();
        var setupSql = File.ReadAllText(GetRootPath("Scripts/SetupDB.sql"));
        var server = new Server(new ServerConnection(connection));
        server.ConnectionContext.ExecuteNonQuery(setupSql);
    }

    private static void DeleteDatabase()
    {
        var deleteDatabaseSql = @"
          IF DB_ID('SUPERGIS') IS NOT NULL
            BEGIN
              ALTER DATABASE SUPERGIS SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
              DROP DATABASE SUPERGIS;
          END;";

        using var connection = new SqlConnection(CreateConnectionString("master"));
        connection.Open();
        using var cmd = new SqlCommand(deleteDatabaseSql, connection);
        cmd.ExecuteNonQuery();
    }

    private static string GetRootPath(string filePath)
    {
        var absolutePath = Path.IsPathRooted(filePath)
            ? filePath
            : Path.GetRelativePath(Directory.GetCurrentDirectory(), filePath);

        return File.Exists(absolutePath)
            ? absolutePath
            : throw new ArgumentException($"Could not find file at path: {absolutePath}");
    }

    private static string CreateConnectionString(string initialCatalog)
        => new SqlConnectionStringBuilder()
        {
            DataSource = "localhost",
            UserID = "sa",
            Password = "myAwesomePassword1",
            InitialCatalog = initialCatalog,
            Encrypt = false,
        }.ToString();
}
