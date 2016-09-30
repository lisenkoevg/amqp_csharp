using System;
using System.IO;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using NamedPipeWrapper;
using System.Management;

public partial class Supervisor
{
    public static string[] cmdArgs;
    public static readonly char paramSeparator = '=';
    public static Dictionary<string,string> cmdArgsDic = new Dictionary<string,string>();
    public static Process currentProcess = Process.GetCurrentProcess();
    private ConcurrentDictionary<int,AMQPManagerProcessWrapper> procList = new ConcurrentDictionary<int,AMQPManagerProcessWrapper>();

    private int managerCheckPeriod = 5000;
    private static int exceptionLogSuffix = 1;
    private static Timer checkTimer;
    private AutoResetEvent mainEvent = new AutoResetEvent(false);
    private AutoResetEvent checkEvent = new AutoResetEvent(true);
    private static bool exitScheduled = false;
    public static Logger logger = new Logger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);
    public static bool isConsoleAvailable = IsConsoleAvailable();
    private static bool isServiceMode = false;
    
    private class AMQPManagerProcessWrapper
    {
        public Process process;
        public AMQPManager.State state;

        public AMQPManagerProcessWrapper(Process process)
        {
            this.process = process;
            if (!process.HasExited)
            {
               state = AMQPManager.State.Running;
            }
            else
            {
               state = AMQPManager.State.Crash;
            }
        }
    }

    public static void Main(string[] args)
    {
        cmdArgs = args;
        Configure();

        if (GetArg("parentPID") != null)
        {
            AMQPManager am = new AMQPManager();
        }
        else
        {
            if (isServiceMode)
            {
                SetupService();
            }
            else
            {
                Supervisor s = new Supervisor();
            }
        }
    }

    public static void Configure()
    {
        ChDir();
        AMQPManager.Configure();
        AMQPManager.PrepareLogDir();
        HandleCommandLine();
    }

    public static void HandleCommandLine()
    {
        if (GetArg("?") != null)
        {
            ShowHelp();
            Environment.Exit(0);
        }
        if (!Environment.UserInteractive
            || GetArg("help") != null
            || GetArg("run") != null
            || GetArg("install") != null
            || GetArg("uninstall") != null
            || GetArg("start") != null
            || GetArg("stop") != null)
        {
            isServiceMode = true;
        }
    }

    public static void ShowHelp()
    {
        String exename = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase).Replace("file:\\", "");
        string msg = string.Format("Command-line options:\n{0} [-config{1}<path to config>] | help | ...", exename, paramSeparator);
        msg += string.Format("\nDefault config \"{0}\"", AMQPManager.configFile);
        msg += string.Format("\nhelp - help about working with service");
        if (Supervisor.isConsoleAvailable)
            Console.WriteLine(msg);
    }

    public static string GetArg(string parameter)
    {
        string result = null;
        for (int i = 0; i < cmdArgs.Length; i++)
        {
            var ar = cmdArgs[i].Split(paramSeparator);
            if (ar[0] == parameter || ar[0] == "-" + parameter || ar[0] == "--" + parameter || ar[0] == "/" + parameter)
            {
                result = (ar.Length > 1) ? ar[1].Trim() : "";
                break;
            }
        }
        return result;
    }

    public static void ArgsToDic()
    {
        for (int i = 0; i < cmdArgs.Length; i++)
        {
            var ar = cmdArgs[i].Split(paramSeparator);
            string parameter = ar[0];
            parameter = Regex.Replace(parameter, "^(\\-\\-|\\-|/)", "");
            string value = (ar.Length > 1) ? ar[1].Trim() : "";
            cmdArgsDic[parameter] = value;
        }
    }

    private static void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;
    }

    public static Process WaitForChildProcess(int parentProcessId, string exeName)
    {
        int tries = 30;
        int i = 0;
        Process result = null;
        do
        {
            Thread.Sleep(200);
            i++;
            result = GetChildProcess(parentProcessId, exeName);
        }
        while (result == null && i < tries);
        if (result == null)
        {
            logger.Log(string.Format("Child process with name={0} with parentProcessId={1} not exists", exeName, parentProcessId));
            Environment.Exit(1);
        }
        return result;
    }

    public static Process GetChildProcess(int parentId, string exeName)
    {
        Process result = null;
        string query = string.Format("SELECT ProcessId FROM Win32_Process WHERE ParentProcessId = {0} and Name = '{1}'", parentId, exeName);
        try
        {
            var search = new ManagementObjectSearcher("root\\CIMV2", query);
            var results = search.Get().GetEnumerator();
            if (results.MoveNext())
            {
                var queryObj = results.Current;
                uint pid = (uint)queryObj["ProcessId"];
                result = Process.GetProcessById((int)pid);
            }
        }
        catch (Exception e)
        {
            string msg = string.Format("WMI error. Can't retreive child process id of parentId={0} with name={1}\n{2}", parentId, exeName, e);
            logger.Log(msg);
        }
        return result;
    }

    public Supervisor()
    {
        if (isServiceMode) return;        
        if (isConsoleAvailable)
        {
            Console.WriteLine("Running in command-line mode. Press Ctrl-C to stop");
            Console.CancelKeyPress += OnConsoleCancel;
            SetConsoleSize();
        }
        Work();
        WaitExit();
    }

    private void Work()
    {
        logger.Log(string.Format("Start Supervisor pid={0}", currentProcess.Id));
        StartPipeServer();
        StartManagers();
        checkTimer = new Timer((obj) => { CheckManagers(); });
        checkTimer.Change(1000, managerCheckPeriod);
    }
    
    private void WaitExit()
    {
        mainEvent.WaitOne();
        StopPipeServer();
        logger.Log(string.Format("Stop Supervisor pid={0}", currentProcess.Id));
    }
    
    private void StartManagers()
    {
        for (int i = 0; i < AMQPManager.managersCount; i++)
        {
            StartManager();
        }
    }

    private void StartManager()
    {
        Process process = AMQPManager.SpawnNewInstance(exceptionLogSuffix++, currentProcess.Id, currentProcess.MainModule.FileName);
        bool added = procList.TryAdd(process.Id, new AMQPManagerProcessWrapper(process));
        logger.Log(string.Format(
            "Add{0} {1} pid={2} (total={3})",
            added ? "" : " TryAdd failed",
            Path.GetFileNameWithoutExtension(currentProcess.MainModule.FileName),
            process.Id,
            procList.Count
        ));
    }

    private void CheckManagers()
    {
        checkEvent.WaitOne();
        List<int> runningManagersPids = new List<int>();
        var lst = procList.Keys.ToList();
        foreach (int pid in lst)
        {
            if (procList[pid].process.HasExited)
            {
                AMQPManagerProcessWrapper p;
                bool removed = procList.TryRemove(pid, out p);
                logger.Log(string.Format(
                    "Removed{0} {1} pid={2} (state={3}) (total={4})",
                    removed ? "" : " (TryRemove failed)",
                    Path.GetFileNameWithoutExtension(currentProcess.MainModule.FileName),
                    pid,
                    p.state,
                    procList.Count
                ));
            }
            else
            {
                if (procList[pid].state == AMQPManager.State.Running)
                    runningManagersPids.Add(pid);
            }
        }
        if (!exitScheduled)
        {
            ProtectiveCheck(runningManagersPids);
        }
        else
        {
            SendStopManager();
            if (procList.Count == 0)
            {
                mainEvent.Set();
            }
        }
        checkEvent.Set();
    }

    private void ProtectiveCheck(List<int> runningManagersPids)
    {
        if (runningManagersPids.Count != AMQPManager.managersCount)
        {
            logger.Log(string.Format(
                "ProtectiveCheck runningManagersCount={0}, managersCount={1}",
                runningManagersPids.Count,
                AMQPManager.managersCount
            ));
            if (runningManagersPids.Count > AMQPManager.managersCount)
            {
                SendStopManager(runningManagersPids[0]);
            }
            else if (runningManagersPids.Count < AMQPManager.managersCount)
            {
                StartManager();
            }
        }
    }

    public static void ScheduleExit()
    {
        if (!exitScheduled)
        {
            exitScheduled = true;
            if (checkTimer != null) checkTimer.Change(0, 1000);
        }
    }

    public static bool IsConsoleAvailable()
    {
        try
        {
            var a = Console.KeyAvailable;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void OnConsoleCancel(object sender, ConsoleCancelEventArgs args)
    {
        if (!exitScheduled)
        {
            ScheduleExit();
            args.Cancel = true;
            Console.WriteLine("Exiting...");
        }
    }

    private void SetConsoleSize()
    {
        try
        {
            Console.WindowHeight = 10;
            Console.BufferHeight = 9999;
        }
        catch {}
    }
}