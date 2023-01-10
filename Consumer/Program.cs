using MessageExplorer;
using Serilog.Events;
using Serilog;
using WorkerService1;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var builder = Host.CreateDefaultBuilder(args);
builder.ConfigureServices((context, services) =>
{
    services.AddHostedService<Consumer>();
    //services.AddMessageExplorer(context.Configuration);
}).UseSerilog();

var app = builder.Build();
app.Run();
