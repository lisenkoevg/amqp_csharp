using System;
using NamedPipeWrapper;

[Serializable]
public class PipeMessage
{
    public int pid;
    public AMQPManager.State state;
    public string description = "";
    
    public override string ToString()
    {
        string s;
        if (description != "")
            s = string.Format("pid={0} AMQPManager.State={1}({2})", pid, state, description);
        else
            s = string.Format("pid={0} AMQPManager.State={1}", pid, state);
        return s;
    }
}
