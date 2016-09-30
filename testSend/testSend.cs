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
        Dictionary<string,dynamic> settings = ConfigLoader.LoadFile("./config/settings.yaml");
        
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = settings["amqp"]["host"],
            Port = Convert.ToInt32(settings["amqp"]["port"]),
            UserName = settings["amqp"]["user"],
            Password = settings["amqp"]["password"],
            VirtualHost = settings["amqp"]["vhost"]
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
            object rpc_id;
            if (new Random().Next(0,2) == 1)
            {
              rpc_id = 12345;
            }
            else
            {
              rpc_id = "12345";
            }
            props.Headers = new Dictionary<string,object>() {{"method", method}, {"rpc_id", rpc_id}};
            ch.BasicPublish(
                exchange: "",
                routingKey: "ax.test",
                basicProperties: props,
                body: body
            );
        }
    }
}
