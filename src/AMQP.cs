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
    public int AMQP_no;
    public bool is_stop_pend = false;
    private bool is_processing = false;
    public DateTime start_time = DateTime.Now;
    public int err_count = 0;
    public int msg_count = 0;
    
    public delegate void AMQPEventHandler(AMQP a);
    public event AMQPEventHandler Stop;
    
    private AxCon ax;
    private static object lockOn = new Object();
    
    private static readonly String queue = "ax.test";
    private static readonly string log_dir = "log";
    private static readonly Dictionary<string,dynamic> config = ConfigLoader.Load("./config");
    private static int _AMQP_no = 1;
    
    public static uint msgInQueue;
    
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
            AMQP_no = _AMQP_no;
            _AMQP_no++;
        }
    }
    
    private void Work()
    {
        ax = new AxCon(config, AMQP_no);
        
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
            EventHandler<BasicDeliverEventArgs> receiveHandler = (model, ea) =>
            {
                is_processing = true;
                try
                {
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
                    msgInQueue = channel.MessageCount(queue);
                }
                catch (Exception e)
                {
                    log(e, "err");
                    err_count++;
                }
                is_processing = false;
            };
            consumer.Received += receiveHandler;
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            channel.BasicConsume(
                queue: queue,
                noAck: false,
                consumer: consumer
            );
            while (!is_stop_pend || is_processing)
            {
                if (is_stop_pend)
                {
                    consumer.Received -= receiveHandler;
                }
                Thread.Sleep(1000);
            }
            ax.Logoff();
        }
        if (Stop != null)
        {
            Stop(this);
        }
    }

    private void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = System.IO.Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;        
    }
    public bool IsAxReady()
    {
        return ax != null;
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
    
    // TODO combine with AxCon.log()
    private void log(object obj, string suf = "")
    {
        string basename = "amqp";
        suf = suf != "" ? "_" + suf : "";
        string file_name = basename + AMQP_no + suf;
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        using (StreamWriter writer = new StreamWriter(log_dir + "/" + file_name + ".log", true))
        {
            writer.WriteLine("{0};{1}", ts, obj.ToString());
        }

    }
}
