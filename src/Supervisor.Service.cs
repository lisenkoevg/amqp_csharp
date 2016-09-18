using System;
using System.IO;
using System.Threading.Tasks;
using Topshelf;

public partial class Supervisor
{
    public static void SetupService()
    {
        string serviceName = Path.GetFileNameWithoutExtension(currentProcess.MainModule.FileName);
        HostFactory.Run(x =>
        {            
            x.Service<Supervisor>(s =>
            {
               s.ConstructUsing(name => new Supervisor());
               s.WhenStarted(tc => tc.Start());
               s.WhenStopped(tc => tc.Stop());
            });
            x.SetServiceName(serviceName);
        });
    }
        
    public void Start()
    {
        Work();
    }

    public void Stop()
    {
        ScheduleExit();
        WaitExit();
    }
}
