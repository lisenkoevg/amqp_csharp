using System;
using NamedPipeWrapper;

public partial class Supervisor
{
    public static string pipeName = "AMQPManagerNamedPipe";
    public static Logger pipeLogger = new Logger("pipe");
    private NamedPipeServer<PipeMessage> pipeServer;
    
    private void StartPipeServer()
    {
        pipeServer = new NamedPipeServer<PipeMessage>(pipeName);
        pipeServer.ClientConnected += OnClientConnected;
        pipeServer.ClientDisconnected += OnClientDisconnected;
        pipeServer.ClientMessage += OnClientMessage;
        pipeServer.Error += OnError;
        pipeServer.Start();
    }
    
    private void StopPipeServer()
    {
        pipeServer.Stop();
    }
    
    private void OnClientMessage(NamedPipeConnection<PipeMessage,PipeMessage> connection, PipeMessage message)
    {
        pipeLogger.Log(string.Format("Pipe: OnClientMessage {0}", message));
        procList[message.pid].state = message.state;
        if (message.state == AMQPManager.State.UserRestart)
        {
            StartManager();
        }
        if (message.state == AMQPManager.State.UserStop)
        {
            SendStopManager();
            ScheduleExit();
        }
    }
    
    private void OnClientConnected(NamedPipeConnection<PipeMessage,PipeMessage> connection)
    {
        pipeLogger.Log(string.Format("Pipe: OnClientConnected client id={0}", connection.Id));
    }
    
    private void OnClientDisconnected(NamedPipeConnection<PipeMessage,PipeMessage> connection)
    {
        pipeLogger.Log(string.Format("Pipe: OnClientDisconnected client id={0}", connection.Id));
    }
    
    private void OnError(Exception e)
    {
        pipeLogger.Log(string.Format("Pipe: {0}.OnError() {1}", this.GetType().Name, e));
    }
    
    private void SendStopManager(int processId = 0)
    {
        var message = new PipeMessage(){
            state = AMQPManager.State.SupervisorStop,
            pid = processId
        };
        pipeLogger.Log(string.Format("Pipe: {0}.StopManager() message={1}", this.GetType().Name, message.ToString()));
        pipeServer.PushMessage(message);
    }
}
