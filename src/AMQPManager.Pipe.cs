using System;
using NamedPipeWrapper;

public partial class AMQPManager
{
    private NamedPipeClient<PipeMessage> pipeClient;
    
    private void StartPipeClient()
    {
        pipeClient = new NamedPipeClient<PipeMessage>(Supervisor.pipeName);
        pipeClient.ServerMessage += OnServerMessage;
        pipeClient.Disconnected += OnDisconnected;
        pipeClient.Error += OnError;
        pipeClient.Start();
    }
    
    private void StopPipeClient()
    {
        pipeClient.Stop();
    }
    
    private void PushOwnState(string descr)
    {
        var msg = new PipeMessage() {
            pid = currentProcess.Id,
            state = this.state,
            description = descr
        };
        string s = string.Format("Pipe: AMQPManager.PushOwnState() {0}", msg.ToString());
        logger.Log(s);
        infoMsg += s + "\n";
        pipeClient.PushMessage(msg);
    }
    
    private void OnServerMessage(NamedPipeConnection<PipeMessage,PipeMessage> connection, PipeMessage message)
    {
        if (message.pid == currentProcess.Id || message.pid == 0)
        {
            string s = string.Format("Pipe: OnServerMessage connection.id={0} message={1}", connection.Id, message.ToString());
            logger.Log(s);
            infoMsg += s + "\n";
            if (state == State.Running && state != message.state)
            {
                state = message.state;
            }
            if (state == State.SupervisorStop)
            {
                ScheduleApplicationExit(restart: false);
            }
        }
    }
    
    private void OnDisconnected(NamedPipeConnection<PipeMessage,PipeMessage> connection)
    {
        Supervisor.pipeLogger.Log(string.Format("Pipe: OnDisconnected id={0}", connection.Id));
    }
    
    private void OnError(Exception e)
    {
        infoMsg += "\n" + e.ToString();
        Supervisor.pipeLogger.Log(string.Format("Pipe: {0}.OnError() {1}", this.GetType().Name, e));
    }
}