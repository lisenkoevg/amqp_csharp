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
    public enum State {Init, Ready, Running, Paused, StopPend, Stopped, InitError};
    public readonly int workerId;
    public DateTime startTime = DateTime.Now;
    public int errorCount = 0;
    public int msgCount = 0;
    public delegate void AMQPEventHandler(AMQP a);
    public delegate Dictionary<string,object> AxRequestHandler(AMQP amqp, string method, Dictionary<string,dynamic> prms, string id);
    public event AMQPEventHandler OnStop;
    public event AMQPEventHandler OnFatalError;
    public event AxRequestHandler OnAxRequest;
    public static uint msgInQueue;
    private static readonly String queue = "ax.test";
    private static readonly Dictionary<string,dynamic> settings = ConfigLoader.LoadFile("./config/settings.yaml");
    private State state = State.Init;
    private bool isProcessing = false;
    private object lockOn = new Object();
    private IConnection connection;
    private IModel channel;
    private EventingBasicConsumer consumer;
    
    public AMQP(int workerId)
    {
        this.workerId = workerId;
    }
    
    public void Init()
    {
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = settings["amqp"]["host"],
            Port = Convert.ToInt32(settings["amqp"]["port"]),
            UserName = settings["amqp"]["user"],
            Password = settings["amqp"]["password"],
            VirtualHost = settings["amqp"]["vhost"]
        };
        try 
        {
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            // channel.QueueDeclare(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += ReceivedHandler;
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            SetState(State.Ready);
        }
        catch (Exception e)
        // catch (BrokerUnreachableException e)
        {
            log(e, "err");
            SetState(State.InitError);
        }
    }
    
    private void ReceivedHandler(object model, BasicDeliverEventArgs ea)
    {
        try
        {
            if (GetState() == State.Paused)
            {
                Thread.Sleep(100);
                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                return;
            }
            msgInQueue = channel.MessageCount(queue);
            isProcessing = true;
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
            Dictionary<string,dynamic> responseObj = OnAxRequest(this, method, request, rpc_id);
            string response = JSON.ToJSON(responseObj);
            msgCount++;
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
            log(e, "err");
            errorCount++;
            OnFatalError(this);
        }
        catch (Exception e)
        {
            log(e, "err");
            errorCount++;
            OnFatalError(this);
        }
        finally
        {
            isProcessing = false;
        }
    }

    public void Start()
    {
        Thread thrd = new Thread(Work);
        thrd.Start();
    }
    
    public void Work()
    {
        try 
        {
            channel.BasicConsume(
                queue: queue,
                noAck: false,
                consumer: consumer
            );
            SetState(State.Running);
            
            // TODO Timer
            while (GetState() == State.Running || GetState() == State.Paused || isProcessing)
            {
                if (!connection.IsOpen)
                {
                    lock(lockOn) state = State.StopPend;
                    OnFatalError(this);
                }
                Thread.Sleep(1000);
            }
            channel.Dispose();
            connection.Dispose();
        }
        // catch (BrokerUnreachableException e)
        catch (Exception e)
        {
            log(e, "err");
            errorCount++;
            OnFatalError(this);
        }
        OnStop.BeginInvoke(this, null, null);
    }
    
    public State GetState()
    {
        lock (lockOn)
            return state;
    }
    
    private void SetState(State state)
    {
        lock (lockOn)
        {
            if (state == State.Ready && this.state != State.Init) return;
            this.state = state;
        }
    }
    
    public void SetInitError()
    {
        SetState(State.InitError);
    }
    
    public void StopPend()
    {
        lock (lockOn)
        {
            state = State.StopPend;
            if (consumer != null)
                consumer.Received -= ReceivedHandler;
        }
    }
    
    public void TogglePaused()
    {
        lock (lockOn)
        {
            if (state == State.Running)
            {
                state = State.Paused;
            }
            else if (state == State.Paused)
            {
                state = State.Running;
            }
        }
    }

    public bool IsProcessing()
    {
        return isProcessing;
    }
    
    private static object lockOnSt = new Object();    
    private void log(object obj, string suf = "")
    {
        string basename = "amqp";
        suf = suf != "" ? "_" + suf : "";
        string file_name = basename + suf;
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        lock (lockOnSt)
        {
            using (StreamWriter writer = new StreamWriter(AMQPManager.logDir + "/" + file_name + ".log", true))
            {
                writer.WriteLine("{0};{1};{2}", ts, workerId, obj.ToString());
            }
        }
    }
}
