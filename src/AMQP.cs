using System;
using System.IO;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using fastJSON;
using System.Diagnostics;
using System.Threading;

public class AMQP
{
    public Thread thrd;
    public bool isRunning = true;
    public int AMQPNo;
    public DateTime startTime = DateTime.Now;
    public int err_count = 0;
    public int msg_count = 0;
    
    private AxCon ax;
    private object lockOn = new Object();
    
    private static String queue = "ax.test";
    private static Dictionary<string,dynamic> config = ConfigLoader.Load("./config");
    private static int _AMQPNo = 1;
    private static string log_dir = "log";
    
    public AMQP()
    {
        Directory.CreateDirectory(log_dir);
        ChDir();
        SetNo();
        thrd = new Thread(this.Work);
        thrd.Start();
    }
    
    private void SetNo()
    {
        lock(lockOn)
        {
            AMQPNo = _AMQPNo;
            _AMQPNo++;
        }
    }
    
    private void Work()
    {
        ax = new AxCon(config, AMQPNo);
        
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
            channel.QueueDeclare(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );
            
            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try{
                    string message = Encoding.UTF8.GetString(ea.Body);
                    
                    IBasicProperties props = ea.BasicProperties;
                    string ReplyTo = props.IsReplyToPresent() ? props.ReplyTo : "";
                    string CorrelationId = props.IsCorrelationIdPresent() ? props.CorrelationId : "";
                    IDictionary<string,dynamic> headers = props.IsHeadersPresent()
						? props.Headers : new Dictionary<string,object>();
                    string method = headers.ContainsKey("method")
						? Encoding.UTF8.GetString(headers["method"]) : "";
                    string rpc_id = headers.ContainsKey("rpc_id")
						? Encoding.UTF8.GetString(headers["rpc_id"]) : "";
                    
                    dynamic request = JSON.Parse(message);
                    
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    Dictionary<string,dynamic> responseObj = ax.request(method, request, rpc_id);
                    string response = JSON.ToJSON(responseObj);
                    msg_count++;
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
                    log(e, "err");
                    err_count++;
                }
            };            
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            channel.BasicConsume(
                queue: queue,
                noAck: false,
                consumer: consumer
            );
            while (isRunning)
            {
                Thread.Sleep(1000);
            }
            ax.Logoff();
        }
    }

    private void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = System.IO.Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;        
    }
    
    public int GetAxErrCount()
    {
        return ax.err_count;
    }
    public int GetAxMsgCount()
    {
        return ax.msg_count;
    }
    public bool GetIsAxRequesting()
    {
        return ax.is_requesting;
    }
    public DateTime GetLastReqStartTime()
    {
        return ax.last_request_starttime;
    }
    public string GetLastMethod()
    {
        return ax.last_method;
    }
    public string GetLongestMethod()
    {
        return ax.longest_method;
    }
    public bool IsAxReady()
    {
        return ax != null;
    }
    
    // TODO combine with AxCon.log()
    private void log(object obj, string suf = "")
    {
        string basename = "amqp";
        suf = suf != "" ? "_" + suf : "";
        string file_name = basename + AMQPNo + suf;
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        using (StreamWriter writer = new StreamWriter(log_dir + "/" + file_name + ".log", true))
        {
            writer.WriteLine("{0};{1}", ts, obj.ToString());
        }

    }
}
