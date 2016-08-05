using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class TestSend
{
    public static void Main(string[] args)
    {
        dynamic config = ConfigLoader.Load("./config");
        
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = config["settings"]["amqp"]["host"],
            Port = Convert.ToInt32(config["settings"]["amqp"]["port"]),
            UserName = config["settings"]["amqp"]["user"],
            Password = config["settings"]["amqp"]["password"],
            VirtualHost = config["settings"]["amqp"]["vhost"]
        };
        
        using (var con = factory.CreateConnection())
        using (var ch = con.CreateModel())
        {
            string method = "";
            string message = "{}";
            if (args.Length > 0) {
                if (args[0] == "purgeQueue"){
                    uint i = ch.QueuePurge("ax.test");
                    Console.WriteLine("{0} messages purged", i);
                    args[0] = "";
                    method = args[1];
                    args[1] = "";
                } else {
                    method = args[0];
                    args[0] = "";
                }
                message = string.Join("", args);
            }
            message = message.Replace("'", "\"");
            Console.WriteLine("method={0}, message={1}", method, message);
            var body = Encoding.UTF8.GetBytes(message);
            IBasicProperties props = ch.CreateBasicProperties();
            props.Headers = new Dictionary<string,object>() {{"method", method}, {"rpc_id", "rpc#1"}};
            ch.BasicPublish(
                exchange: "",
                routingKey: "ax.test",
                basicProperties: props,
                body: body
            );
        }
    }
}
