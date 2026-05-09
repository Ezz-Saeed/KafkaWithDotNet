using Confluent.Kafka;

namespace KafkaDtonet.Producer
{
    public class ProducerService
    {
        private readonly ILogger<ProducerService> logger;

        public ProducerService(ILogger<ProducerService> logger)
        {
            this.logger = logger;
        }

        public async Task ProduceAsync(CancellationToken cancellationToken)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
                AllowAutoCreateTopics = true,
                Acks = Acks.All
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                var result = await producer.ProduceAsync(topic: "test-topic", new Message<Null, string>
                {
                    Value = $"Hello, Kafka! {DateTime.UtcNow}"
                }, cancellationToken);
                logger.LogInformation($"Delivered message to: {result.Value} offset: {result.Offset}");
            }
            catch (ProduceException<Null, string> e)
            {
                logger.LogError($"Failed to deliver message: {e.Error.Reason}");
            }

            producer.Flush(cancellationToken);
        }
    }
}
