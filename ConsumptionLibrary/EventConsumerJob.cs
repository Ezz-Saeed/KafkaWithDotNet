using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumptionLibrary
{
    public class EventConsumerJob : BackgroundService
    {
        private readonly ILogger<EventConsumerJob> logger;
        private readonly KafkaConsumerSettings settings;

        public EventConsumerJob(ILogger<EventConsumerJob> logger, IOptions<KafkaConsumerSettings> settings)
        {
            this.logger = logger;
            this.settings = settings.Value;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = settings.BootstrapServers,
                GroupId = settings.GroupId,
                AutoOffsetReset = settings.AutoOffsetReset,
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(settings.Topic);
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
