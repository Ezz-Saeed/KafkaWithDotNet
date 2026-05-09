using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaotNet.Consumer
{
    internal class EventConsumerJob : BackgroundService
    {
        private readonly ILogger<EventConsumerJob> logger;

        public EventConsumerJob(ILogger<EventConsumerJob> logger)
        {
            this.logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe("test-topic");
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(5));
                    if (result == null)
                    {
                        continue;
                    }
                    logger.LogInformation($"Consumed message: '{result.Message.Value}' at '{result.Offset}'");
                }
            }
            catch (Exception ex)
            {

            }
            return Task.CompletedTask;
        }
    }
}
