using Akka.Actor;

namespace AkkaSample1;

public static class AkkaDemo
{
    public static async Task RunAsync()
    {
        using var actorSystem = ActorSystem.Create("demo-system");
        var greeter = actorSystem.ActorOf(Props.Create(() => new GreeterActor()), "greeter");

        var message = new Greet("Akka.NET");
        var response = await greeter.Ask<Greeted>(message, TimeSpan.FromSeconds(3));

        Console.WriteLine(response.Text);
    }
}

public sealed record Greet(string Name);
public sealed record Greeted(string Text);

public sealed class GreeterActor : ReceiveActor
{
    public GreeterActor()
    {
        Receive<Greet>(greet =>
        {
            Sender.Tell(new Greeted($"Cześć, {greet.Name}! Wiadomość od aktora."));
        });
    }
}
