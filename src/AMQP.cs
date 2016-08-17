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
    public enum State {Init, Connect, Ready, Running, Paused, StopPend, InitError, Stopped};
    public readonly int workerId;
    public DateTime startTime = DateTime.Now;
    public int errorCount = 0;
    public int msgCount = 0;
    // public delegate void AMQPEventHandler(AMQP a);
    // public event AMQPEventHandler OnStop;
    public delegate Dictionary<string,object> AxRequestHandler(AMQP amqp, string method, Dictionary<string,dynamic> prms, string id);
    public event AxRequestHandler OnAxRequest;
    public static uint msgInQueue;
    private bool asyncInitTimedOut = false;
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
        var t1 = DateTime.Now;
        ConnectionFactory factory = new ConnectionFactory() {
            HostName = settings["amqp"]["host"],
            Port = Convert.ToInt32(settings["amqp"]["port"]),
            UserName = settings["amqp"]["user"],
            Password = settings["amqp"]["password"],
            VirtualHost = settings["amqp"]["vhost"]
        };
        try 
        {
            SetState(State.Connect);
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            // channel.QueueDeclare(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += ReceivedHandler;
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            lock (lockOn)
            {
                if (!GetAsyncInitTimedOut())
                {
                    SetState(State.Ready);
                }
                else
                {
                    errorCount++;
                }
            }
        }
        catch (Exception e)
        // catch (BrokerUnreachableException e)
        {
            SetState(State.InitError);
            log(e, "err");
            errorCount++;
        }
        dbg.fa("amqp.Init() " + workerId + " time=" + (DateTime.Now-t1).TotalMilliseconds.ToString("0") + " " + GetState() + " asyncRes=" + GetAsyncInitTimedOut());
    }
    
    private void ReceivedHandler(object model, BasicDeliverEventArgs ea)
    {
        State state = GetState();
        try
        {
            if (state == State.Paused || state == State.StopPend || state == State.Stopped)
            {
                Thread.Sleep(100);
                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                return;
            }
            msgInQueue = channel.MessageCount(queue);
            SetIsProcessing(true);
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
        // catch (AlreadyClosedException e)
        catch (Exception e)
        {
            log(e, "err");
            errorCount++;
        }
        finally
        {
            SetIsProcessing(false);
        }
    }

    public void Start()
    {
        Thread thrd = new Thread(Work);
        thrd.Start();
    }
    
    private void Work()
    {
        try 
        {
            channel.BasicConsume(
                queue: queue,
                noAck: false,
                consumer: consumer
            );
            SetState(State.Running);
        }
        // catch (BrokerUnreachableException e)
        catch (Exception e)
        {
            log(e, "err");
            errorCount++;
        }
            
        while (GetState() == State.Running || GetState() == State.Paused || IsProcessing())
        {
            if (!connection.IsOpen)
            {
                SetState(State.StopPend);
                errorCount++;
            }
            Thread.Sleep(1000);
        }
        channel.Dispose();
        connection.Dispose();
        SetState(State.Stopped);
    }
    
    public bool GetAsyncInitTimedOut()
    {
        lock (lockOn) return asyncInitTimedOut;
    }
    
    public void SetAsyncInitTimedOut(bool val)
    {
        lock (lockOn) asyncInitTimedOut = val;
    }
    
    public State GetState()
    {
        lock (lockOn) return state;
    }
    
    private void SetState(State state)
    {
        lock (lockOn)
        {
            this.state = state;
        }
    }
        
    public void SetInitState()
    {
        SetState(State.Init);
        try { channel.Dispose(); } catch {}
        try { connection.Dispose(); } catch {}
    }
    
    public void StopPend()
    {
        if (consumer != null)
        {
            consumer.Received -= ReceivedHandler;
        }
        lock (lockOn)
        {
            State state = GetState();
            if (state != State.Running && state != State.Paused)
            {
                // Needed because in Init() factory.CreateConnection() called without using(var connection = factory.CreateConnection())
                // and prevent Thread to release Console when exiting application
                // Thread.CurrentThread.IsBackground = true - doesn't help when Init() called via object Task.
                try { channel.Dispose(); } catch {}
                try { connection.Dispose(); } catch {}
                SetState(State.Stopped);
            }
            SetState(State.StopPend);
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
        lock (lockOn)
            return isProcessing;
    }
    
    private void SetIsProcessing(bool val)
    {
        lock (lockOn)
            isProcessing = val;
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
