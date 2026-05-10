using ConsumptionLibrary;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
namespace KafkaotNet.Consumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Start consuming events.......");

            var builder = Host.CreateApplicationBuilder(args);

            builder.Services.Configure<KafkaConsumerSettings>(options =>
            {
                options.BootstrapServers = "localhost:9092";
                options.GroupId = "test-group";
                options.Topic = "test-topic";
                options.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            });

            builder.Services.AddHostedService<EventConsumerJob>();
            builder.Build().Run();
        }
    }
}
