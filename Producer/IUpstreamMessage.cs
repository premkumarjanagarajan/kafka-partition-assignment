using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace WorkerService2;

public interface IUpstreamMessage { }

public class UpstreamMessage : IUpstreamMessage
{
    public string Message { get; set; }
}
public class MessageDeserializer : IDeserializer<IUpstreamMessage>
{
    public IUpstreamMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        return (IUpstreamMessage)JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data), typeof(UpstreamMessage))!;
    }
}

