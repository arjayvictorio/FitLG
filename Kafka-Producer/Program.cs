using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text.Json;
using System.Text.Json.Serialization;

string TheNewTopic = "ZapFitness";

//a1:

//Console.Write("What topic to use?");
//TheNewTopic = Console.ReadLine();

//if (TheNewTopic == "" || TheNewTopic == null)
//{
//    goto a1;
//}

var config = new ProducerConfig 
{ 
    BootstrapServers = "pkc-3w22w.us-central1.gcp.confluent.cloud:9092",
    SecurityProtocol=SecurityProtocol.SaslSsl,
    SaslMechanism=SaslMechanism.Plain,
    SaslUsername= "NXU4YYQZM36FDOAK",
    SaslPassword= "Z4dMj0XcTuTg99a7MBFJRA5aGUBa71xif5iAf8aymN8f0zoKSBzku2FDJW61IgZk"
};

using var producer = new ProducerBuilder<Null, string>(config).Build();



using (var adminClient = new AdminClientBuilder(config).Build())
{
    try
    {
        await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = TheNewTopic, ReplicationFactor = 3, NumPartitions = 6 } });
    }
    catch (CreateTopicsException e)
    {
        Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
    }
}

Console.WriteLine("---Start---");

try
{
    string? state;
    while ((state = Console.ReadLine()) != null)
    {
        var response = await producer.ProduceAsync(TheNewTopic,
            new Message<Null, string>
            {
                Value = JsonSerializer.Serialize<Weather>(new Weather(state, 70))
            });

        Console.WriteLine(response.Value);
    }
}
catch (ProduceException<Null,string> exc)
{
    Console.WriteLine(exc.Message);
}

public record Weather(string State, int Temperature);
