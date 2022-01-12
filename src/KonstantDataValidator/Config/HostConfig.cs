// using KonstantDataValidator.Change;
// using Microsoft.Extensions.Configuration;
// using Microsoft.Extensions.DependencyInjection;
// using Microsoft.Extensions.Hosting;
// using Serilog;
// using Serilog.Events;
// using Serilog.Formatting.Compact;

// namespace KonstantDataValidator.Config;

// public static class HostConfig
// {
//     public static IHost Configure()
//     {
//         var hostBuilder = new HostBuilder();
//         ConfigureApp(hostBuilder);
//         ConfigureLogging(hostBuilder);
//         ConfigureServices(hostBuilder);
//         return hostBuilder.Build();
//     }

//     private static void ConfigureApp(IHostBuilder hostBuilder)
//     {
//         hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
//         {
//             config.AddJsonFile("AppSettings.json");
//         });
//     }

//     private static void ConfigureServices(IHostBuilder hostBuilder)
//     {
//         hostBuilder.ConfigureServices((hostContext, services) =>
//         {
//             services.AddOptions();
//             services.AddHostedService<KonstantValidatorHost>();
//             services.AddTransient<IChangeEventListen, ChangeEventListen>();
//             services.Configure<Settings>(s => hostContext.Configuration.GetSection("Settings").Bind(s));
//         });
//     }

//     private static void ConfigureLogging(IHostBuilder hostBuilder)
//     {
//         hostBuilder.ConfigureServices((hostContext, services) =>
//         {
//             var loggingConfiguration = new ConfigurationBuilder()
//                .AddEnvironmentVariables().Build();

//             services.AddLogging(loggingBuilder =>
//             {
//                 var logger = new LoggerConfiguration()
//                     .ReadFrom.Configuration(loggingConfiguration)
//                     .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
//                     .MinimumLevel.Override("System", LogEventLevel.Warning)
//                     .Enrich.FromLogContext()
//                     .WriteTo.Console(new CompactJsonFormatter())
//                     .CreateLogger();

//                 loggingBuilder.AddSerilog(logger, true);
//             });
//         });
//     }
// }