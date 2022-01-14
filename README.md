# Arcgis integrator

## Example

Example can be found [here.](https://github.com/DAXGRID/arcgis-integrator/tree/master/src/ArcgisIntegrator.Console)

## Publish example app

Example publishing to win-x64.

```sh
dotnet publish src/ArcgisIntegrator.Console/ArcgisIntegrator.Console.csproj --sc --runtime win-x64 -o ./publish
```

## Running database for tests in docker.

```sh
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=myAwesomePassword1" -e "MSSQL_AGENT_ENABLED=True"  -p 1433:1433 -d  mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04
```

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
