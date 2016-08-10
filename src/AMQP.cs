using System;
using System.IO;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Events;
using fastJSON;
using System.Diagnostics;
using System.Threading;

public class AMQP
{
    public enum State {Running, Paused, StopPend};
    
    public Thread thrd;
    public int AMQP_no;
    public bool is_stop_pend = false;
    private bool is_processing = false;
    public DateTime start_time = DateTime.Now;
    public int err_count = 0;
    public int msg_count = 0;
    
    public delegate void AMQPEventHandler(AMQP a);
    public event AMQPEventHandler OnStop;
    public event AMQPEventHandler OnFatalError;
    
    private AxCon axcon;
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
        axcon = new AxCon(config, AMQP_no, AxConFatalHandler);
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = config["settings"]["amqp"]["host"],
            Port = Convert.ToInt32(config["settings"]["amqp"]["port"]),
            UserName = config["settings"]["amqp"]["user"],
            Password = config["settings"]["amqp"]["password"],
            VirtualHost = config["settings"]["amqp"]["vhost"]
        };
        try 
        {
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                /*
                channel.QueueDeclare(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );
                */
                
                EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                EventHandler<BasicDeliverEventArgs> receiveHandler = (model, ea) =>
                {
                    try
                    {
                        msgInQueue = channel.MessageCount(queue);
                        is_processing = true;
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
                        Dictionary<string,dynamic> responseObj = axcon.request(method, request, rpc_id);
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
                    catch (AlreadyClosedException e)
                    {
                        log(string.Format("Caught {0}", e.GetType()), "err");
                        err_count++;
                        OnFatalError(this);
                    }
                    catch (Exception e)
                    {
                        log(e, "err");
                        err_count++;
                        OnFatalError(this);
                    }
                    finally
                    {
                        is_processing = false;
                    }
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
                    if (!connection.IsOpen)
                    {
                        is_stop_pend = true;
                        OnFatalError(this);
                    }
                    // TODO to timer
                    Thread.Sleep(1000);
                }
            }
        }
        catch (BrokerUnreachableException e)
        {
            log(string.Format("Caught {0}", e.GetType()), "err");
            err_count++;
            OnFatalError(this);
        }
        catch (Exception e)
        {
            log(e, "err");
            err_count++;
            OnFatalError(this);
        }
        try
        {
            axcon.Logoff();
        }
        catch {}
        OnStop(this);
    }

    private void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = System.IO.Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;        
    }
    
    private void AxConFatalHandler()
    {
        is_stop_pend = true;
        OnFatalError(this);
    }
    
    public bool IsProcessing()
    {
        return is_processing;
    }
    
    public bool IsAxReady()
    {
        return axcon != null;
    }
    
    public Dictionary<string,dynamic> GetAxInfo()
    {
        var result = new Dictionary<string,object>()
        {
            {"err_count", 0},
            {"msg_count", 0},
            {"is_requesting", false},
            {"is_logged_on", false},
            {"last_request_starttime", new DateTime()},
            {"last_method", ""},
            {"longest_method", ""},
            {"longest_method_duration", new TimeSpan()}
        };
        if (IsAxReady())
        {
            result = axcon.GetInfo();
        }
        return result;
    }
    
    // TODO combine with AxCon.log()
    private void log(object obj, string suf = "")
    {
        string basename = "amqp";
        suf = suf != "" ? "_" + suf : "";
        string file_name = basename + suf;
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        lock (lockOn)
        {
            using (StreamWriter writer = new StreamWriter(log_dir + "/" + file_name + ".log", true))
            {
                writer.WriteLine("{0};{1};{2}", ts, AMQP_no, obj.ToString());
            }
        }
    }
}
