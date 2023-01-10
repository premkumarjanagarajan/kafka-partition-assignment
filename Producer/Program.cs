using WorkerService2;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<Producer>();
    })
    .Build();

host.Run();
