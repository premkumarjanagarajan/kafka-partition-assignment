namespace WorkerService1;

public delegate Task RequestDelegate(MessageContext context);

public class MessageContext
{
    public Dictionary<string, string> Headers { get; set; }
    public UpstreamMessage Message { get; set; }
}


public class MessageProcessorBuilder
{
    private readonly LinkedList<MessageProcessorComponent> _components = new();

    public void Use(Func<RequestDelegate, RequestDelegate> component)
    {
        var node = new MessageProcessorComponent
        {
            Component = component
        };

        _components.AddLast(node);
    }

    public RequestDelegate Build()
    {
        var node = _components.Last;
        while (node != null)
        {
            node.Value.Next = GetNextFunc(node);
            node.Value.Process = node.Value.Component(node.Value.Next);
            node = node.Previous;
        }
        return _components.First.Value.Process;
    }

    private static RequestDelegate GetNextFunc(LinkedListNode<MessageProcessorComponent> node)
    {
        return node.Next == null 
            ? _ => Task.CompletedTask // no more middleware components left in the list 
            : node.Next.Value.Process;
    }

    private class MessageProcessorComponent
    {
        public RequestDelegate Next;
        public RequestDelegate Process;
        public Func<RequestDelegate, RequestDelegate> Component;
    }

}