using System;
using System.IO;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using fastJSON;
using System.Diagnostics;

public class AMQP
{
    private AxCon ax;
    private String queue;
    private Dictionary<string,dynamic> config;

    public static void Main(string[] args)
    {
        var amqp = new AMQP();
        amqp.Work();
    }

    public AMQP()
    {
        ChDir();
        queue = "ax.sync";
        config = ConfigLoader.Load("./config");
    }

    public void Work()
    {
        ax = new AxCon(config);
        
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = config["settings"]["amqp"]["host"],
            Port = Convert.ToInt32(config["settings"]["amqp"]["port"]),
            UserName = config["settings"]["amqp"]["user"],
            Password = config["settings"]["amqp"]["password"],
            VirtualHost = config["settings"]["amqp"]["vhost"]
        };
        using (IConnection connection = factory.CreateConnection())
        using (IModel channel = connection.CreateModel())
        {
            // channel.QueueDeclare(
                // queue: queue,
                // durable: true,
                // exclusive: false,
                // autoDelete: false,
                // arguments: null
            // );
            
            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try{
                    string message = Encoding.UTF8.GetString(ea.Body);
                    Console.Write("Received message={0}, ea.DeliveryTag={1}", message, ea.DeliveryTag);
                    
                    IBasicProperties props = ea.BasicProperties;
                    string ReplyTo = props.IsReplyToPresent() ? props.ReplyTo : "";
                    string CorrelationId = props.IsCorrelationIdPresent() ? props.CorrelationId : "";
                    IDictionary<string,dynamic> headers = props.IsHeadersPresent() ? props.Headers : new Dictionary<string,object>();
                    string method = headers.ContainsKey("method") ? Encoding.UTF8.GetString(headers["method"]) : "";
                    string rpc_id = headers.ContainsKey("rpc_id") ? Encoding.UTF8.GetString(headers["rpc_id"]) : "";
                    // key(user_hash) приходит из Body
                    Console.WriteLine(" method={0}, rpc_id={1}", method, rpc_id);
                    
                    dynamic request = JSON.Parse(message);
                    
                    //*** for testing ***
                    if (method != "item_info")
                    {
                        System.Threading.Thread.Sleep(100);
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        if (ea.DeliveryTag > 10) {
                            Environment.Exit(0);
                        }
                        return;
                    }
                    //*******************
                    
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    Dictionary<string,dynamic> responseObj = ax.request(method, request, rpc_id);
                    string response = JSON.ToJSON(responseObj);
                    if (ReplyTo != "" && CorrelationId != "")
                    {
                        props = channel.CreateBasicProperties();
                        props.CorrelationId = CorrelationId;
                        channel.BasicPublish(
                            exchange: "",
                            routingKey: ReplyTo,
                            basicProperties: props,
                            body: Encoding.UTF8.GetBytes(response)
                        );
                    }
                }
                catch (Exception e)
                {
                    dbg.f(e, "OnReceive_Exception");
                }
            };            
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            channel.BasicConsume(
                queue: queue,
                noAck: false,
                consumer: consumer
            );
            
            // System.Threading.Thread.Sleep(10000);
            Console.WriteLine("Waiting for messages... Press [enter] to exit.");
            Console.ReadLine();
        }
    }

    private void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = System.IO.Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;        
    }
}
