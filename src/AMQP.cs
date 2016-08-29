using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
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
    public enum State {Init, Connect, InitError, Ready, Running, Paused, ForcePaused, Error, StopPend, Stopped};
    public readonly int workerId;
    public DateTime startTime = DateTime.Now;
    public int errorCount = 0;
    public int msgCount = 0;
    public delegate AxCon.RequestState AxPrepareRequestHandler(AMQP amqp, string method, Dictionary<string,dynamic> prms, string id);
    public delegate Dictionary<string,object> AxExecuteRequestHandler(AMQP amqp, string method, Dictionary<string,dynamic> prms, string id);
    public event AxPrepareRequestHandler OnAxPrepareRequest;
    public event AxExecuteRequestHandler OnAxExecuteRequest;
    public static uint msgInQueue;
    private bool asyncInitTimedOut = false;
    private static readonly Dictionary<string,dynamic> settings = ConfigLoader.LoadFile("./config/settings.yaml");
    public static readonly String queue = settings["amqp"]["queue"];
    private State state = State.Init;
    private bool isProcessing = false;
    private object lockOn = new Object();
    private IConnection connection;
    private IModel channel;
    private EventingBasicConsumer consumer;
    private string consumerTag;
    
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
            if (!GetAsyncInitTimedOut())
            {
                SetState(State.InitError);
            }
            log(e, "error");
            errorCount++;
        }
        // dbg.fa(string.Format(
            // "amqp.Init() {0} time={1} {2} asyncTimedOut={3}",
            // workerId,
            // (DateTime.Now-t1).TotalMilliseconds.ToString("0"),
            // GetState(),
            // GetAsyncInitTimedOut()
        // ));
    }

    public void Start()
    {
        Thread thrd = new Thread(Work);
        thrd.Start();
    }

    private void Work()
    {
        if (StartConsume())
        {
            SetState(State.Running);
        }

        var state = GetState();
        while (state == State.Running || state == State.Paused || state == State.ForcePaused || IsProcessing())
        {
            if (!connection.IsOpen || !channel.IsOpen)
            {
                log("Connection and/or channel are closed " + state, "error");
                StopConsume();
                SetState(State.Error);
                errorCount++;
            }
            Thread.Sleep(1000);
            state = GetState();
        }

        TryDisconnect();
        if (GetState() != State.Error)
        {
            SetState(State.Stopped);
        }
    }

    private bool StartConsume()
    {
        bool result = false;
        lock (lockOn)
        {
            if (connection.IsOpen && channel.IsOpen)
            {
                if (consumerTag == "" || consumerTag == null)
                {
                    try
                    {
                        consumerTag = channel.BasicConsume(
                            queue: queue,
                            noAck: false,
                            consumer: consumer
                        );
                        result = true;
                    }
                    catch (Exception e)
                    {
                        SetState(State.Error);
                        errorCount++;
                        log(e, "error");
                    }
                }
                else
                {
                    dbg.fa("StartConsume() without corresponding StopConsume()");
                }
            }
            else
            {
                log("Connection and/or channel are closed " + GetState(), "error");
                SetState(State.Error);
                errorCount++;
            }
        }
        return result;
    }
    
    private bool StopConsume()
    {
        bool result = false;
        lock (lockOn)
        {
            if (connection.IsOpen && channel.IsOpen)
            {
                if (consumerTag != "")
                {
                    try
                    {
                        channel.BasicCancel(consumerTag);
                        consumerTag = "";
                        result = true;
                    }
                    catch (Exception e)
                    {
                        SetState(State.Error);
                        errorCount++;
                        log(e, "error");
                    }
                }
                else
                {
                    dbg.fa("StopConsume() without corresponding StartConsume()");
                }
            }
            else
            {
                log("Connection and/or channel are closed " + GetState(), "error");
                SetState(State.Error);
                errorCount++;
            }
        }
        return result;
    }
    
    private void ReceivedHandler(object model, BasicDeliverEventArgs ea)
    {
        // EventingBasicConsumer consumer = (EventingBasicConsumer) model;
        // IModel channel = consumer.Model;
        StringBuilder msg = new StringBuilder();
        try
        {
            msgInQueue = channel.MessageCount(queue);
            string message = (ea.Body != null && ea.Body.Length != 0) ? Encoding.UTF8.GetString(ea.Body) : "{}";
            IBasicProperties props = ea.BasicProperties;
            string ReplyTo = props.IsReplyToPresent() ? props.ReplyTo : "";
            string CorrelationId = props.IsCorrelationIdPresent() ? props.CorrelationId : "";
            IDictionary<string,dynamic> headers = props.IsHeadersPresent()
                ? props.Headers : new Dictionary<string,object>();
            string method = headers.ContainsKey("method") && headers["method"] != null
                ? Encoding.UTF8.GetString(headers["method"]) : "";
            string rpc_id = headers.ContainsKey("rpc_id") && headers["rpc_id"] != null
                ? Encoding.UTF8.GetString(headers["rpc_id"]) : "";

            State state = GetState();
            msg.AppendFormat(
                "st={0};method={1};rpc_id={2};ReplyTo={3};CorId={4};msg={5}",
                state, method, rpc_id, ReplyTo, CorrelationId,
                Regex.Replace(message, "(user_hash(?:[^0-9,a-f]{3,10})[0-9,a-f]{10})([0-9,a-f]{22})", "$1*", RegexOptions.IgnoreCase)
            );
            if (state != State.Paused && state != State.ForcePaused && state == State.Running)
            {
                isProcessing = true;
                Dictionary<string,object> responseObj = new Dictionary<string,object>();
                dynamic request = null;
                AxCon.RequestState axRequestState = AxCon.RequestState.NotApplicable;
                try
                {
                    request = JSON.Parse(message);
                }
                catch
                {
                    responseObj["result"] = null;
                    responseObj["error"] = new Dictionary<string,object>(){
                        {"code", -32600},
                        {"message", "Invalid Request. The JSON sent is not a valid Request object"}
                    };
                    responseObj["elapsed"] = 0;
                    msg.Insert(0, "invalid request;");
                }
                if (request != null)
                {
                    axRequestState = OnAxPrepareRequest(this, method, request, rpc_id);
                    msg = msg.Insert(0, string.Format("reqSt={0};", axRequestState));
                }
                if (axRequestState == AxCon.RequestState.PrepOk || axRequestState == AxCon.RequestState.PrepWarn || request == null)
                {
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    msg.Insert(0, "ack;");
                    if (request != null)
                    {
                        responseObj = OnAxExecuteRequest(this, method, request, rpc_id);
                    }
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
                        msg.Insert(0, "publish;");
                    }
                    msgCount++;
                }
                else if (axRequestState == AxCon.RequestState.PrepErr)
                {
                    channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                    msg.Insert(0, "Nack;");
                }
            }
            else
            {
                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                msg.Insert(0, "Nack;");
            }
        }
        // catch (AlreadyClosedException e)
        catch (Exception e)
        {
            StopConsume();
            SetState(State.Error);
            log(e, "error");
            errorCount++;
            msg.Insert(0, "st=State.Error;");
        }
        finally
        {
            isProcessing = false;
            log(msg,"request");
        }
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
        TryDisconnect();
        SetState(State.Init);
    }

    public void ForcePause()
    {
        lock (lockOn)
        {
            if (StopConsume())
            {
                SetState(State.ForcePaused);
            }
        }
    }

    public void ForceResume()
    {
        lock (lockOn)
        {
            if (StartConsume())
            {
                SetState(State.Running);
            }
        }
    }

    public void TryDisconnect()
    {
        try
        {
            if (channel != null && channel.IsOpen)
            {
                channel.Dispose();
            }
            if (connection != null && connection.IsOpen)
            {
                connection.Dispose();
            }
        }
        catch (Exception e)
        {
            dbg.fa(e);
        }
    }

    public void StopPend()
    {
        StopConsume();
        lock (lockOn)
        {
            State state = GetState();
            if (state != State.Running && state != State.Paused && state != State.ForcePaused && state != AMQP.State.Connect)
            {
                TryDisconnect();
                SetState(State.Stopped);
            }
            else
            {
                SetState(State.StopPend);
            }
        }
    }

    public void TogglePaused()
    {
        lock (lockOn)
        {
            if (state == State.Running)
            {
                StopConsume();
                state = State.Paused;
            }
            else if (state == State.Paused)
            {
                StartConsume();
                state = State.Running;
            }
        }
    }

    public bool IsProcessing()
    {
        lock (lockOn)
            return isProcessing;
    }

    private static object lockOnSt = new Object();
    public void log(object obj, string fileSuffix = "")
    {
        fileSuffix = (fileSuffix != "") ? "_" + fileSuffix : "";
        DateTime dtNow = DateTime.Now;
        string timestamp = dtNow.ToString("yyyy-MM-dd HH:mm:ss");
        string basename = this.GetType().Name;
        string file_name = basename + fileSuffix + ".log";
        string dir = AMQPManager.logDir + "\\" + dtNow.ToString("yyyyMMdd");
        Directory.CreateDirectory(dir);
        
        lock (lockOnSt)
        {
            using (StreamWriter writer = new StreamWriter(dir + "\\" + file_name, true))
            {
                writer.WriteLine("{0};{1,3};{2}", timestamp, workerId, obj.ToString());
            }
        }
    }
}
