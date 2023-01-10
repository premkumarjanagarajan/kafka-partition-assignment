using Confluent.Kafka;

namespace WorkerService1
{
    public class Consumer : BackgroundService
    {
        private readonly ILogger<Consumer> _logger;
        private readonly IConsumer<Ignore, IUpstreamMessage> _consumer;
        private readonly string topicName1 = "cgtest4";
        private readonly string topicName2 = "cgtest2";
        private readonly string topicName3 = "cgtest3";
        private readonly IAdminClient _adminClient;

        public Consumer(ILogger<Consumer> logger)
        {
            _logger = logger;
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "cgtest-group4",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = true
            };

            IDeserializer<IUpstreamMessage> deserializer = new MessageDeserializer();
            _consumer = new ConsumerBuilder<Ignore, IUpstreamMessage>(consumerConfig)
                .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                .SetValueDeserializer(deserializer).Build();
            _adminClient = new AdminClientBuilder(consumerConfig).Build();
        }
        
        private void OnPartitionsAssigned(IConsumer<Ignore, IUpstreamMessage> arg1, List<TopicPartition> assigned)
        {
            _logger.LogInformation("Number of assigned partitions {@total}", assigned.Count);
            foreach (TopicPartition tp in assigned)
            {
                _logger.LogInformation("Assigning topic partition pair {topic}:{partition}", tp.Topic, tp.Partition);
            }
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Run(() =>
            {
                _consumer.Subscribe(new List<string> { topicName1, topicName2, topicName3 });
                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumeResults = new List<ConsumeResult<Ignore, IUpstreamMessage>>();

                    for (int i = 0; i < 500; i++)
                    {
                        var consumeResult = _consumer.Consume(100);
                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            break;
                        }

                        _logger.LogInformation($"Consumed message with offset {consumeResult.Offset}");
                        consumeResults.Add(consumeResult);
                    }

                    if (!consumeResults.Any()) continue;
                    
                    var latestConsumeResults = consumeResults
                        .GroupBy(consumeResult => consumeResult.TopicPartition)
                        .Select(grouping => grouping.OrderByDescending(consumeResult => consumeResult.TopicPartitionOffset.Offset.Value).First())
                        .ToList();

                    foreach (var consumeResult in latestConsumeResults.Where(consumeResult =>
                                 _consumer.Assignment.Contains(consumeResult.TopicPartition)))
                    {
                        _consumer.StoreOffset(consumeResult);
                    }
                }
            }, stoppingToken);

            return Task.CompletedTask;
        }
    }
}