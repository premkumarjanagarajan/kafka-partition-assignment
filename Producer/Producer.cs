using Confluent.Kafka;
using Newtonsoft.Json;

namespace WorkerService2;

public class Producer : BackgroundService
{
    private readonly ILogger<Producer> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly Random _random;

    public Producer(ILogger<Producer> logger)
    {
        _logger = logger;
        _random = new Random();
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(60000, stoppingToken);
            var str = "hello" + _random.Next();
            var message = new UpstreamMessage
            {
                Message = str
            };
            var result = await _producer.ProduceAsync("workertest1", new Message<string, string>
            {
                Value = JsonConvert.SerializeObject(message)
            }, stoppingToken);
            _logger.LogInformation($"Produced message with offset {result.Offset}");
        }
    }
}