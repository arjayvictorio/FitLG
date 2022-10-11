using Confluent.Kafka;
using System.Text.Json;

var config = new ConsumerConfig
{
    GroupId = "weather-consumer-group",
    BootstrapServers = "pkc-3w22w.us-central1.gcp.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "NXU4YYQZM36FDOAK",
    SaslPassword = "Z4dMj0XcTuTg99a7MBFJRA5aGUBa71xif5iAf8aymN8f0zoKSBzku2FDJW61IgZk",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();

string? TheNewTopic = "ZapFitness";

//a1:

//Console.Write("What topic to use?");
//TheNewTopic = Console.ReadLine();

//if (TheNewTopic == "" || TheNewTopic == null)
//{
//    goto a1;
//}

consumer.Subscribe(TheNewTopic);

CancellationTokenSource token = new();

try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);
        if (response.Message!=null)
        {
            var weather = JsonSerializer.Deserialize<Weather>(response.Message.Value);
            Console.WriteLine($"City: {weather.State}, Temp: {weather.Temperature}F");
        }
    }
}
catch (Exception)
{

	throw;
}

public record Weather(string State, int Temperature);