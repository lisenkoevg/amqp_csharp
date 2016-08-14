using System;
using System.Collections.Generic;
using System.Threading;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Diagnostics;
using System.IO;
using fastJSON;

public class AMQPManager
{
    private int _workerId = 0;
    private int maxWorkersCount = 30;
    private int amqpInitTimeout = 10000;
    private int axconInitTimeout = 20000;
    private int axconRequestTimeout = 30000;
    private int startupWorkersCount;
    private readonly Process cur_proc = Process.GetCurrentProcess();
    private readonly Dictionary<string,dynamic> config = ConfigLoader.Load("./config");
    private readonly int fatalErrorThreshold = 5;
    private readonly DateTime startTime = DateTime.Now;
    private readonly bool isConsoleAvailable;
    private Dictionary<int, AMQP> amqpDic = new Dictionary<int, AMQP>();
    private Dictionary<int, AxCon> axconDic = new Dictionary<int, AxCon>();
    private Timer outputTimer;
    private Timer inputTimer;
    private AutoResetEvent waitOutputHandle = new AutoResetEvent(false);
    private AutoResetEvent waitInputHandle = new AutoResetEvent(false);
    private int workerCount = 0;
    private bool outputPaused = false;
    private bool inputPaused = true;
    private string userInput = "";
    private int updateScreenPeriod = 500;
    private int inputPollPeriod = 200;
    private int fatalErrorCount = 0;
    private Dictionary<int,Dictionary<string,int>> stat = new Dictionary<int,Dictionary<string,int>>();
    private object lockOn = new object();
    private StringBuilder output = new StringBuilder();
    
    public static readonly string logDir = "log";
    
    public static void Main(string[] args)
    {
        ChDir();
        Directory.CreateDirectory(logDir);
        
        int count;
        AMQPManager am;
        if (args.Length > 0 && Int32.TryParse(args[0], out count))
        {
            am = new AMQPManager(count);
        }
        else
        {
            am = new AMQPManager(0);
        }
    }

    public AMQPManager(int count)
    {
        Configure(ref count);
        
        isConsoleAvailable = IsConsoleAvailable();
        var thrd = new Thread(Work);
        thrd.Start(count);
        
        outputTimer = new Timer(
            (obj) => {
                if (!outputPaused) Print();
            },
            null,
            0,
            updateScreenPeriod
        );
        waitOutputHandle.WaitOne();
        outputTimer.Dispose();
        Print();
    }
    
    private void Configure(ref int count)
    {
        var conf = ConfigLoader.LoadFile("./config/managerConfig.yaml");
        int parsedValue = 0;
        
        if (conf.ContainsKey("maxWorkersCount") && Int32.TryParse(conf["maxWorkersCount"], out parsedValue))
        {
            maxWorkersCount = parsedValue;
        }
        if (count == 0 &&
            conf.ContainsKey("startupWorkersCount") && Int32.TryParse(conf["startupWorkersCount"], out parsedValue)
            )
        {
            count = parsedValue;
        }
        if (count < 0 || count > maxWorkersCount)
        {
            count = 1;
        }
        startupWorkersCount = count;
        if (conf.ContainsKey("amqpInitTimeout") && Int32.TryParse(conf["amqpInitTimeout"], out parsedValue))
        {
            amqpInitTimeout = parsedValue;
        }
        if (conf.ContainsKey("axconInitTimeout") && Int32.TryParse(conf["axconInitTimeout"], out parsedValue))
        {
            axconInitTimeout = parsedValue;
        }
        if (conf.ContainsKey("axconRequestTimeout") && Int32.TryParse(conf["axconRequestTimeout"], out parsedValue))
        {
            axconRequestTimeout = parsedValue;
        }
    }
    
    private void Work(object count)
    {
        inputTimer = new Timer(
            (obj) => {
                if (isConsoleAvailable && Console.KeyAvailable) {
                    ConsoleKeyInfo ki = Console.ReadKey(outputPaused || inputPaused);
                    if (!inputPaused)
                        HandleInput(ki);
                };
            },
            null,
            0,
            inputPollPeriod
        );
        AddWorkers((int)count);
        inputPaused = false;
        waitInputHandle.WaitOne();
        inputTimer.Dispose();
    }
    
    private void AddWorkers(int count)
    {
        for (int i = 0; i < count; i++)
        {
            AddWorker();
        }
    }
    
    private void AddWorker()
    {
        if (GetRunningWorkerCount() >= maxWorkersCount)
            return;
        int workerId = GetWorkerId();
        var amqp = new AMQP(workerId);
        var axcon = new AxCon(config, workerId);
        lock (lockOn)
        {
            amqpDic.Add(workerId, amqp);
            axconDic.Add(workerId, axcon);
            workerCount++;
        }
        amqp.OnStop += DeleteWorker;
        
        var t1 = DateTime.Now;
        IAsyncResult amqpAsyncResult = new Action(amqp.Init).BeginInvoke((obj) => {dbg.fa("amqp init time=" + (DateTime.Now-t1).TotalMilliseconds.ToString("0"));}, null);
        
        bool amqpWaitRes = amqpAsyncResult.AsyncWaitHandle.WaitOne(amqpInitTimeout, true);
        if (amqpWaitRes && amqp.GetState() == AMQP.State.Ready)
        {
            
            var t2 = DateTime.Now;
            IAsyncResult axconAsyncResult = new Action(axcon.Init).BeginInvoke((obj) => {dbg.fa("axcon init time=" + (DateTime.Now-t2).TotalMilliseconds.ToString("0"));}, null);
            bool axconWaitRes = axconAsyncResult.AsyncWaitHandle.WaitOne(axconInitTimeout, true);
            if (axconWaitRes && axcon.GetState() == AxCon.State.Ready)
            {
                amqp.OnAxRequest += AxRequest;
                amqp.Start();
            }
            else
            {
                dbg.fa(string.Format("axcon init failed {0} state={1}", workerId, axcon.GetState()));
                fatalErrorCount++;
            }
        }
        else
        {
            dbg.fa(string.Format("amqp init failed {0} state={1}", workerId, amqp.GetState()));
            amqp.SetInitError();
            fatalErrorCount++;
        }
    }
    
    private void DeleteWorker(AMQP a)
    {
        //logoff asynchronously, exceptions is suppressed
        new Action(axconDic[a.workerId].Logoff).BeginInvoke(null, null);
        lock (lockOn)
        {
            amqpDic.Remove(a.workerId);
            axconDic.Remove(a.workerId);
            workerCount--;
        }
    }
    
    private int GetWorkerId()
    {
        return Interlocked.Increment(ref _workerId);
    }
    
    private Dictionary<string,object> AxRequest(AMQP amqp, string method, Dictionary<string,dynamic> prms, string id)
    {
        var result = new Dictionary<string,object>();
        result = axconDic[amqp.workerId].request(method, prms, id);
        return result;
    }
    
    private void FatalErrorHandler(AMQP a)
    {
        fatalErrorCount++;
        if (fatalErrorCount < fatalErrorThreshold)
        {
            AddWorker();
        }
        else
        {
            waitInputHandle.Set();
        }
    }
    
    private void StopWorker()
    {
        lock (lockOn)
        {
            var list = amqpDic.Keys.ToList();
            list.Sort();
            foreach (var workerId in list)
            {
                var amqp = amqpDic[workerId];
                var state = amqp.GetState();
                if (state == AMQP.State.Init
                    || state == AMQP.State.Ready
                    || state == AMQP.State.Running
                    || state == AMQP.State.Paused
                    || state == AMQP.State.InitError)
                {
                    amqp.StopPend();
                    break;
                }
            }
        }
    }
    
    private void StopAllWorkers()
    {
        lock (lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                amqp.StopPend();
            }
        }
    }
    
    private void StopWorkerById(int workerId)
    {
        lock (lockOn)
        {
            if (amqpDic.ContainsKey(workerId))
            {                
                amqpDic[workerId].StopPend();
            }
        }
    }
    
    private void RestartAllWorkers()
    {
        int cur_worker_count = GetRunningWorkerCount();
        StopAllWorkers();
        AddWorkers(cur_worker_count);
    }

    private void TogglePausedForAllWorkers()
    {
        lock (lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                amqp.TogglePaused();
            }
        }
    }
    
    private void TogglePausedForWorkerById(int workerId)
    {
        lock(lockOn)
        {
            if (amqpDic.ContainsKey(workerId))
            {                
                amqpDic[workerId].TogglePaused();
            }
        }
    }
        
    private int GetRunningWorkerCount()
    {
        int result = 0;
        lock(lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                var state = amqp.GetState();
                if (state == AMQP.State.Init
                    || state == AMQP.State.Ready
                    || state == AMQP.State.Running
                    || state == AMQP.State.Paused
                    || state == AMQP.State.InitError)
                {
                    result++;
                }
            }
        }
        return result;
    }

    private void HandleInput(ConsoleKeyInfo ki)
    {
        string keyChar = ki.KeyChar.ToString();
        string keyStr = ki.Key.ToString();
        if (outputPaused && keyStr != "Spacebar")
        {
            return;
        }
        if (userInput == "")
        {
            switch (keyStr)
            {
                case "A":
                case "D":
                    ExecuteCommand(keyStr);
                    break;
                case "R":
                case "Q":
                case "P":
                    if ((ki.Modifiers & ConsoleModifiers.Control) != 0)
                    {
                        ExecuteCommand(keyStr);
                    }
                    break;
                case "Spacebar":
                    ToggleOutputPaused();
                    break;
                default:
                    if (Regex.IsMatch(keyChar, "[0-9a-zA-Z]"))
                    {
                        userInput += keyChar;
                    }
                    break;
            }
        }
        else
        {
            switch (ki.Key)
            {
                case ConsoleKey.Backspace:
                    userInput = userInput.Remove(userInput.Length-1);
                    break;
                case ConsoleKey.Spacebar:
                    ToggleOutputPaused();
                    userInput = "";
                    break;
                case ConsoleKey.Escape:
                    userInput = "";
                    break;
                default:
                    if (Regex.IsMatch(keyChar, "[0-9a-zA-Z]"))
                    {
                        userInput += keyChar;
                        if (Regex.IsMatch(userInput.ToUpper(), @"\d*D|\d*P"))
                        {
                            ExecuteCommand(userInput);
                            userInput = "";
                        }
                    }
                    break;
            }
        }
    }
    
    private void ExecuteCommand(string cmd)
    {
        cmd = cmd.ToUpper();
        if (Regex.IsMatch(cmd, @"A|\d*D|\d*P|R|Q"))
        {
            inputPaused = true;
            switch (cmd)
            {
                case "A":
                    AddWorkers(1);
                    break;
                case "D":
                    StopWorker();
                    break;
                case "R":
                    RestartAllWorkers();
                    break;
                case "P":
                    TogglePausedForAllWorkers();
                    break;
                case "Q":
                    StopAllWorkers();
                    waitInputHandle.Set();
                    waitOutputHandle.Set();
                    break;
                default:
                    if (Regex.IsMatch(cmd, @"\d+D"))
                    {
                        int workerId = Convert.ToInt32(cmd.Replace("D", ""));
                        StopWorkerById(workerId);
                    }
                    if (Regex.IsMatch(cmd, @"\d+P"))
                    {
                        int workerId = Convert.ToInt32(cmd.Replace("P", ""));
                        TogglePausedForWorkerById(workerId);
                    }
                    break;
            }
            if (cmd != "Q")
            {
                inputPaused = false;
            }
        }
    }
    
    private void ToggleOutputPaused()
    {
        string cap = "(paused) ";
        outputPaused = !outputPaused;
        if (outputPaused)
            Console.Title = cap + Console.Title;
        else
            Console.Title = Console.Title.Replace(cap, "");
    }
    
    private void Print()
    {
        if (!isConsoleAvailable)
        {
            return;
        }
        if (!Monitor.TryEnter(lockOn))
        {
            dbg.fa("Print() TryEnter failed");
            return;
        }
        
        output.Clear();
        string tmpl = "{0,4} {1,-14} {2,4} {3,5} {4,4} {5,5} {6,-10} {7,4} {8,-9} {9,5} {10,-9} {11,6} {12,-20} {13,-20} {0,4}";
        string head = string.Format(
            tmpl,
            "no",
            "startTime",
            "aMsg",
            "axMsg",
            "aErr",
            "axErr",
            "state",
            "proc",
            "axState",
            "axReq",
            "lastReqSt",
            "reqDur",
            "reqClass",
            "longest"
        );
        output.AppendLine(head);
        output.AppendLine(string.Format("{0}", "".PadLeft(head.Length, '=')));
        
        Dictionary<string,int> total;
        int count = 0;
        List<int> list;
        string objCount;
        try
        {
            list = amqpDic.Keys.ToList();
            list.Sort();
            foreach (var workerId in list)
            {
                AMQP amqp = null;
                AxCon axcon = null;
                if (amqpDic.ContainsKey(workerId))
                {
                    amqp = amqpDic[workerId];
                    axcon = axconDic[workerId];
                }
                if (amqp == null) continue;
                var axInfo = axcon.GetInfo();
                double current_req_duration = axInfo["lastRequestStarttime"] != default(DateTime)
                    ? System.Math.Round((DateTime.Now - axInfo["lastRequestStarttime"]).TotalMilliseconds/1000, 1)
                    : 0;
                output.AppendLine(string.Format(
                    tmpl,
                    amqp.workerId,
                    amqp.startTime.ToString("MM-dd HH:mm:ss"),
                    amqp.msgCount,
                    axInfo["msgCount"],
                    amqp.errorCount,
                    axInfo["errorCount"],
                    amqp.GetState(),
                    amqp.IsProcessing() ? "yes" : "",
                    axcon.GetState(),
                    axInfo["isRequesting"] ? "yes" : "",
                    axInfo["lastRequestStarttime"] != default(DateTime) ? axInfo["lastRequestStarttime"].ToString("HH:mm:ss") : "",
                    axInfo["isRequesting"] && current_req_duration > 0 ? current_req_duration.ToString("0.0") : "",
                    axInfo["isRequesting"] ? axInfo["lastMethod"].Replace("cmpECommerce", "").Trim('_') : "",
                    axInfo["longestMethod"].Replace("cmpECommerce", "").Trim('_')
                ));
                count++;
            }
            objCount = string.Format("a={0} ax={1} w={2}", amqpDic.Count, axconDic.Count, workerCount);
            SaveStat();
            total = CalcTotalStat();
        }
        finally
        {
            Monitor.Exit(lockOn);
        }
        output.AppendLine(string.Format(
            "\nRefresh period: {0}s, running: {1:d\\.hh\\:mm\\:ss}",
            System.Math.Round(updateScreenPeriod / 1000.0, 1),
            (DateTime.Now - startTime))
        );
        output.AppendLine(string.Format(
            "Summary: amqpMsg={0} axMsg={1} amqpErr={2} axErr={3} workers={4} {9} heap={5:0.0}MB private={6:0.0}MB msgInQueue={7} fatal_err={8}",
            total["amqpMsgCount"],
            total["axMsgCount"],
            total["amqpErrorCount"],
            total["axErrorCount"],
            count,
            System.Math.Round(Convert.ToSingle(GC.GetTotalMemory(false)) / 1024 / 1024, 1),
            System.Math.Round(Convert.ToSingle(cur_proc.PrivateMemorySize64) / 1024 / 1024, 1),
            AMQP.msgInQueue.ToString(),
            fatalErrorCount,
            objCount
        ));
        output.AppendLine(string.Format(
            "Config: amqpInitTimeout={0}s axconInitTimeout={1}s axconRequestTimeout={2}s startupWorkersCount={3}",
            amqpInitTimeout / 1000.0,
            axconInitTimeout / 1000.0,
            axconRequestTimeout / 1000.0,
            startupWorkersCount
        ));
        output.AppendLine("\n?: a: start new worker, [id]d: stop worker [by id], Ctrl-r: restart all workers, Ctrl-q: stop all workers and exit");
        output.AppendLine("?: <id>p: pause/resume worker by <id>, Ctrl-p: pause/resume all workers, k: kill worker by id (not yet implemented)");
        output.AppendLine("Space: pause screen update, Ctrl-C: force exit");
        Console.Clear();
        Console.Write(output.ToString());
        Console.Write("{0}", userInput);
    }
    
    private void SaveStat()
    {
        foreach (var workerId in amqpDic.Keys)
        {
            AMQP amqp = null;
            AxCon axcon = null;
            if (amqpDic.ContainsKey(workerId))
            {
                amqp = amqpDic[workerId];
                axcon = axconDic[workerId];
            }
            if (amqp == null) continue;
            if (!stat.ContainsKey(workerId))
            {
                stat.Add(
                    workerId,
                    new Dictionary<string,int>(){
                        {"amqpMsgCount", 0},
                        {"axMsgCount", 0},
                        {"amqpErrorCount", 0},
                        {"axErrorCount", 0}
                    }
                 );
            }
            stat[workerId]["amqpMsgCount"] = amqp.msgCount;
            stat[workerId]["axMsgCount"] = axcon.msgCount;
            stat[workerId]["amqpErrorCount"] = amqp.errorCount;
            stat[workerId]["axErrorCount"] = axcon.errorCount;
        }
    }
    
    private Dictionary<string,int> CalcTotalStat()
    {
        Dictionary<string,int> result = 
            new Dictionary<string,int>(){
                {"amqpMsgCount", 0},
                {"axMsgCount", 0},
                {"amqpErrorCount", 0},
                {"axErrorCount", 0}
            };
        foreach (var i in stat)
        {
            result["amqpMsgCount"] += i.Value["amqpMsgCount"];
            result["axMsgCount"] += i.Value["axMsgCount"];
            result["amqpErrorCount"] += i.Value["amqpErrorCount"];
            result["axErrorCount"] += i.Value["axErrorCount"];
        }
        return result;
    }
    
    private bool IsConsoleAvailable()
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
    
    private static void ChDir()
    {
        String path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
        String directory = System.IO.Path.GetDirectoryName(path).Replace("file:\\", "");
        Environment.CurrentDirectory = directory;        
    }
}
