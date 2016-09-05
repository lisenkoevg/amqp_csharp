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
    public delegate Dictionary<string,object> AxExecuteRequestHandler(AMQP amqp, string id);
    public event AxPrepareRequestHandler OnAxPrepareRequest;
    public event AxExecuteRequestHandler OnAxExecuteRequest;
    public static uint msgInQueue;
    private bool asyncInitTimedOut = false;
    private static readonly Dictionary<string,dynamic> settings = ConfigLoader.LoadFile("./config/settings.yaml");
    public static readonly String queue = settings["amqp"]["queue"];
    private State state = State.Init;
    private bool isProcessing = false;
    private static int pid = Process.GetCurrentProcess().Id;
    private object lockOn = new Object();
    private IConnection connection;
    private IModel channel;
    private EventingBasicConsumer consumer;
    private string consumerTag;
    private Timer workTimer;
    public static Logger logger = new Logger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);
    
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
            logger.Log(workerId, e, "error");
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
        var workAutoResevEvent = new AutoResetEvent(false);
        workTimer = new Timer((obj) => {
            var st = GetState();
            if (!(st == State.Running
                || st == State.Paused
                || st == State.ForcePaused
                || IsProcessing()))
            {
                StopConsume();
                workTimer.Change(Timeout.Infinite, Timeout.Infinite);
                workAutoResevEvent.Set();
            }
            if (!(connection.IsOpen && channel.IsOpen) && !(st == State.Stopped || st == State.StopPend))
            {
                SetState(State.Error);
            }
        }, null, 0, 1000);
        workAutoResevEvent.WaitOne();
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
                        logger.Log(workerId, e, "error");
                    }
                }
                else
                {
                    dbg.fa("Start_Consume() without corresponding Stop_Consume()");
                }
            }
            else
            {
                logger.Log(workerId, "Start_Consume() Connection and/or channel are closed st=" + GetState(), "error");
                SetState(State.Error);
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
                        logger.Log(workerId, e, "error");
                    }
                }
                else
                {
                    // dbg.fa("Stop_Consume() without corresponding Start_Consume()");
                }
            }
            else
            {
                logger.Log(workerId, "Stop_Consume() Connection and/or channel are closed st=" + GetState() + " consumerTag=" + consumerTag, "error");
                consumerTag = "";
                SetState(State.Error);
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
            isProcessing = true;
            var message = UnwrapMessage(ea);

            State state = GetState();
            msg.AppendFormat(
                "st={0};method={1};rpc_id={2};ReplyTo={3};CorId={4};msg={5};debug={6}",
                state, message["method"], message["rpc_id"], message["ReplyTo"], message["CorrelationId"],
                Util.CutUserHash(message["message"]), message["debug"]
            );
            if (channel.IsOpen)
            {
                if (state != State.Paused && state != State.ForcePaused && state == State.Running)
                {
                    Dictionary<string,object> responseObj = new Dictionary<string,object>();
                    dynamic request = null;
                    AxCon.RequestState axRequestState = AxCon.RequestState.NotApplicable;
                    try
                    {
                        request = JSON.Parse(message["message"]);
                        if (!(request is IDictionary))
                        {
                            request = new Dictionary<string,object>();
                        }
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
                        axRequestState = OnAxPrepareRequest(this, message["method"], request, message["rpc_id"]);
                        msg.Insert(0, string.Format("reqSt={0};", axRequestState));
                    }
                    if (axRequestState == AxCon.RequestState.PrepOk || axRequestState == AxCon.RequestState.PrepWarn || request == null)
                    {
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        msg.Insert(0, "ack;");
                        if (request != null)
                        {
                            responseObj = OnAxExecuteRequest(this, message["rpc_id"]);
                        }
                        string response = JSON.ToJSON(responseObj);
                        if (message["ReplyTo"] != "" && message["CorrelationId"] != "")
                        {
                            IBasicProperties props = channel.CreateBasicProperties();
                            props.CorrelationId = message["CorrelationId"];
                            channel.BasicPublish(
                                exchange: "",
                                routingKey: message["ReplyTo"],
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
                msgInQueue = channel.MessageCount(queue);
            }
            else
            {
                msg.Insert(0, "channel closed;");
            }
        }
        // catch (AlreadyClosedException e)
        catch (Exception e)
        {
            SetState(State.Error);
            errorCount++;
            logger.Log(workerId, e, "error");
            msg.Insert(0, string.Format("st={0};", State.Error));
        }
        finally
        {
            isProcessing = false;
            logger.Log(workerId, msg, "request");
        }
    }

    private Dictionary<string,string> UnwrapMessage(BasicDeliverEventArgs ea)
    {
        var result = new Dictionary<string,string>();
        result["message"] = "{}";
        result["debug"] = "";
        if (ea.Body != null)
        {
            if (ea.Body is byte[])
            {
                result["message"] = Encoding.UTF8.GetString(ea.Body);
            }
            else
            {
                result["debug"] += "body is " + ea.Body.GetType().Name;
                result["debug"] += string.Format(" ({0})", ea.Body.ToString());
            }
        }
        IBasicProperties props = ea.BasicProperties;
        result["ReplyTo"] = props.IsReplyToPresent() ? props.ReplyTo : "";
        result["CorrelationId"] = props.IsCorrelationIdPresent() ? props.CorrelationId : "";
        
        result["method"] = "";
        result["rpc_id"] = "";
        if (props.IsHeadersPresent())
        {
            IDictionary<string,object> headers = props.Headers;
            if (headers.ContainsKey("method") && headers["method"] != null)
            {
                if (headers["method"] is byte[])
                {
                    result["method"] = Encoding.UTF8.GetString((byte[])headers["method"]);
                }
                else
                {
                    result["debug"] += ", headers[method] is " + headers["method"].GetType().Name;
                    result["debug"] += string.Format(" ({0})", headers["method"].ToString());
                }
            }
            if (headers.ContainsKey("rpc_id") && headers["rpc_id"] != null)
            {
                if (headers["rpc_id"] is byte[])
                {
                    result["rpc_id"] = Encoding.UTF8.GetString((byte[])headers["rpc_id"]);
                }
                else
                {
                    result["debug"] += ", headers[rpc_id] is " + headers["rpc_id"].GetType().Name;
                    result["debug"] += string.Format(" ({0})", headers["rpc_id"].ToString());
                }
            }
        }
        return result;
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
        lock (lockOn)
        {
            State state = GetState();
            if (state != State.Running && state != State.Paused && state != State.ForcePaused && state != State.Connect)
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
}
