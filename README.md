# Arcgis integrator

You can get the NuGet package [here.](https://www.nuget.org/packages/DAX.ArcgisIntegrator/)

## Examples

An example can be found [here.](https://github.com/DAXGRID/arcgis-integrator/tree/master/examples/ArcgisIntegrator.Console)

## Tests

### Running all tests

```sh
dotnet test
```

### Running unit tests

```sh
dotnet test --filter Category=Unit
```

### Running integration tests

```sh
dotnet test --filter Category=Integration
```

To run the integration it requires a running MS-SQL database. You can use the docker command below to setup a local MS-SQL database to run the integration tests up against.

```sh
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=myAwesomePassword1" -e "MSSQL_AGENT_ENABLED=True"  -p 1433:1433 -d  mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04
```
