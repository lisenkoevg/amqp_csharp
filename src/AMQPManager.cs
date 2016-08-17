using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Diagnostics;
using System.IO;
using fastJSON;

public class AMQPManager
{
    public static readonly string logDir = "log";
    private int _workerId = 0;
    private int maxWorkersCount = 30;
    private int amqpInitTimeout = 15000;
    private int axconInitTimeout = 30000;
    private int axconRequestTimeout = 60000;
    private int workersCheckPeriod = 60000;
    private int startupWorkersCount;
    private readonly Process cur_proc = Process.GetCurrentProcess();
    private readonly Dictionary<string,dynamic> config = ConfigLoader.Load("./config");
    private readonly DateTime startTime = DateTime.Now;
    private readonly bool isConsoleAvailable;
    private Dictionary<int, AMQP> amqpDic = new Dictionary<int, AMQP>();
    private Dictionary<int, AxCon> axconDic = new Dictionary<int, AxCon>();
    private Timer outputTimer;
    private Timer inputTimer;
    private Timer checkWorkersTimer;
    private AutoResetEvent waitOutputHandle = new AutoResetEvent(false);
    private AutoResetEvent waitInputHandle = new AutoResetEvent(false);
    private AutoResetEvent checkWorkersHandle = new AutoResetEvent(false);
    private int workersCount = 0;
    private bool outputPaused = false;
    private bool inputPaused = true;
    private bool isWorkersChecking = false;
    private bool isWorkersRestarting = false;
    private string userInput = "";
    private int updateScreenPeriod = 500;
    private int inputPollPeriod = 100;
    private DateTime nextWorkersCheck = default(DateTime);
    private Dictionary<int,Dictionary<string,int>> stat = new Dictionary<int,Dictionary<string,int>>();
    private object lockOn = new object();
    private StringBuilder output = new StringBuilder();    
    private Task asyncTaskQueueHead = new Task(()=>{});
    private Task asyncTaskQueueTail = null;
    
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
        
        outputTimer = new Timer((obj) => {if (!outputPaused) Print();}, null, 0, updateScreenPeriod);
        waitOutputHandle.WaitOne();
        try { outputTimer.Dispose(); } catch {};
        outputPaused = true;
        Thread.Sleep(1);
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
        CreateWorkers((int)count);
        CheckWorkers();
        int secondCheckTimeout = 15000;
        checkWorkersTimer = new Timer(
            (obj) => {
                nextWorkersCheck = default(DateTime);
                CheckWorkers();
                nextWorkersCheck = DateTime.Now.AddMilliseconds(workersCheckPeriod);
            },
            null,
            secondCheckTimeout,
            workersCheckPeriod
        );
        inputPaused = false;
        waitInputHandle.WaitOne();
        checkWorkersHandle.WaitOne();
        try { inputTimer.Dispose(); } catch {};
        try { checkWorkersTimer.Dispose(); } catch {};
    }
    
    private void CreateWorkers(int count)
    {
        for (int i = 0; i < count; i++)
        {
            CreateWorker();
        }
    }

    private void CreateWorker()
    {
        if (GetWorkersCount() >= maxWorkersCount) return;
        lock (lockOn)
        {
            int workerId = GetWorkerId();
            var amqp = new AMQP(workerId);
            var axcon = new AxCon(config, workerId);
            amqpDic.Add(workerId, amqp);
            axconDic.Add(workerId, axcon);
            workersCount++;
            // amqp.OnStop += DeleteWorker;
        }
    }

    private void CheckWorkers()
    {
        if (isWorkersChecking) return;
        isWorkersChecking = true;
        List<int> list;
        lock (lockOn)
        {
            list = amqpDic.Keys.ToList();
        }
        list.Sort();
        if (IsAcynQueueCompleted())
            InitAsyncQueue();
        {
        }
        foreach (int workerId in list)
        {
            CheckWorker(workerId);
        }
        if (IsAsyncQueueReady())
        {
            asyncTaskQueueHead.Start();
        }
        isWorkersChecking = false;
    }
    
    private void CheckWorker(int workerId)
    {
        AMQP amqp;
        AxCon axcon;
        AMQP.State amqpState;
        AxCon.State axconState;
        AxCon.RequestState requestState;
        bool amqpAsyncInitTimedOut;
        bool axconAsyncInitTimedOut;
        bool isProcessing;
        lock(lockOn)
        {
            if (amqpDic.ContainsKey(workerId))
            {
                amqp = amqpDic[workerId];
                axcon = axconDic[workerId];
                amqpState = amqp.GetState();
                axconState = axcon.GetState();
                amqpAsyncInitTimedOut = amqp.GetAsyncInitTimedOut();
                axconAsyncInitTimedOut = axcon.GetAsyncInitTimedOut();
                isProcessing = amqp.IsProcessing();
                requestState = axcon.GetRequestState();
            }
            else
            {
                return;
            }
        }
        if (amqpState == AMQP.State.Init)
        {
            QueueInitWorkerAMQP(amqp);
        }
        if (axconState == AxCon.State.Init)
        {
            if (amqpState != AMQP.State.StopPend && amqpState != AMQP.State.Stopped)
                QueueInitWorkerAxCon(axcon);
        }
        if (amqpState == AMQP.State.Ready && axconState == AxCon.State.Ready)
        {
            StartWorker(amqp);
        }
        if (amqpState == AMQP.State.Connect && amqpAsyncInitTimedOut)
        {
            lock (lockOn)
            {
                amqp.SetAsyncInitTimedOut(false);
                amqp.SetInitState();
            }
        }
        if (amqpState == AMQP.State.InitError)
        {
            lock (lockOn)
            {
                amqp.SetAsyncInitTimedOut(false);
                amqp.SetInitState();
            }
        }

        if (axconState == AxCon.State.Login && axconAsyncInitTimedOut)
        {
            QueueFinWorkerAxCon(axcon);
        }
        if (axconState == AxCon.State.InitError)
        {
            QueueFinWorkerAxCon(axcon);
        }
        if (axconState == AxCon.State.Logoff && axconAsyncInitTimedOut)
        {
            lock (lockOn)
            {
                axcon.SetAsyncInitTimedOut(false);
                axcon.SetInitState();
            }
        }
        if (axconState == AxCon.State.FinError)
        {
            lock (lockOn)
            {
                axcon.SetAsyncInitTimedOut(false);
                axcon.SetInitState();
            }
        }
        
        if (amqpState == AMQP.State.StopPend || amqpState == AMQP.State.Stopped)
        {
            if (!isProcessing && requestState != AxCon.RequestState.Request)
            {
                if (axconState == AxCon.State.Ready)
                {
                    QueueFinWorkerAxCon(axcon);
                }
                else
                {
                    if (amqpState == AMQP.State.Stopped)
                    {
                        amqp.SetAsyncInitTimedOut(false);
                        axcon.SetAsyncInitTimedOut(false);
                        DeleteWorker(amqp);
                    }
                }
            }
        }
    }

    private bool IsAcynQueueCompleted()
    {
        return asyncTaskQueueTail != null && asyncTaskQueueTail.Status == TaskStatus.RanToCompletion;
    }
    
    private void InitAsyncQueue()
    {
        asyncTaskQueueHead.Dispose();
        asyncTaskQueueTail.Dispose();
        asyncTaskQueueHead = new Task(()=>{});
        asyncTaskQueueTail = null;
    }
    
    private bool IsAsyncQueueReady()
    {
        return asyncTaskQueueHead.Status == TaskStatus.Created && asyncTaskQueueTail != null;
    }

    private bool IsAsyncQueueRunning()
    {
        return asyncTaskQueueTail != null
            && asyncTaskQueueHead.Status == TaskStatus.RanToCompletion
            && asyncTaskQueueTail.Status != TaskStatus.RanToCompletion;
    }
    
    private void QueueInitWorkerAMQP(AMQP amqp)
    {
        if (!IsAsyncQueueRunning())
        {
            asyncTaskQueueTail = asyncTaskQueueHead.ContinueWith(
                (t,obj) => {
                    amqp.SetAsyncInitTimedOut(false);
                    Task task = Task.Run((Action)amqp.Init);
                    bool waitRes = task.Wait(amqpInitTimeout);
                    amqp.SetAsyncInitTimedOut(!waitRes);
                    task.Wait();
                },
                null,
                TaskContinuationOptions.ExecuteSynchronously
            );
        }
    }
    
    private void QueueInitWorkerAxCon(AxCon axcon)
    {
        if (!IsAsyncQueueRunning())
        {
            asyncTaskQueueTail = asyncTaskQueueHead.ContinueWith(
                (t,obj) => {
                    axcon.SetAsyncInitTimedOut(false);
                    Task task = Task.Run((Action)axcon.Init);
                    bool waitRes = task.Wait(axconInitTimeout);
                    axcon.SetAsyncInitTimedOut(!waitRes);
                    task.Wait();
                },
                null,
                TaskContinuationOptions.ExecuteSynchronously
            );
        }
    }
    
    private void QueueFinWorkerAxCon(AxCon axcon)
    {
        if (!IsAsyncQueueRunning())
        {
            asyncTaskQueueTail = asyncTaskQueueHead.ContinueWith(
                (t,obj) => {
                    axcon.SetAsyncInitTimedOut(false);
                    Task task = Task.Run((Action)axcon.Fin);
                    bool waitRes = task.Wait(axconInitTimeout);
                    axcon.SetAsyncInitTimedOut(!waitRes);
                    task.Wait();
                },
                null,
                TaskContinuationOptions.ExecuteSynchronously
            );
        }
        
    }
    
    private void StartWorker(AMQP amqp)
    {
        amqp.OnAxRequest += AxRequest;
        amqp.Start();
    }

    private void DeleteWorker(AMQP a)
    {
        lock (lockOn)
        {
            amqpDic.Remove(a.workerId);
            axconDic.Remove(a.workerId);
            workersCount--;
        }
    }
    
    private int GetWorkerId()
    {
        _workerId++;
        return _workerId;
    }
    
    private Dictionary<string,object> AxRequest(AMQP amqp, string method, Dictionary<string,object> prms, string id)
    {
        var result = new Dictionary<string,dynamic>();
        
        // AxCon axcon = axconDic[amqp.workerId];
        // axcon.SetAsyncRequestTimedOut(false);
        // Task task = Task.Run((Action)amqp.Init);
        // bool waitRes = task.Wait(axconRequestTimeout);
        // axcon.SetAsyncInitTimedOut(!waitRes);
        // task.Wait();

        result = axconDic[amqp.workerId].request(method, prms, id);
        return result;
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
                if (state != AMQP.State.StopPend && state != AMQP.State.Stopped)
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
    
    private void RestartRunningWorkers()
    {
        if (isWorkersRestarting) return;
        isWorkersRestarting = true;
        int currentWorkersCount = GetRunningWorkersCount();
        lock (lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                var state = amqp.GetState();
                if (state == AMQP.State.Running || state == AMQP.State.Paused)
                {
                    StopWorkerById(amqp.workerId);
                }
            }
        }
        CreateWorkers(currentWorkersCount);
        isWorkersRestarting = false;
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

    private int GetWorkersCount()
    {
        int result = 0;
        lock(lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                var state = amqp.GetState();
                if (state != AMQP.State.StopPend && state != AMQP.State.Stopped)
                {
                    result++;
                }
            }
        }
        return result;
    }

    private int GetRunningWorkersCount()
    {
        int result = 0;
        lock(lockOn)
        {
            foreach (var amqp in amqpDic.Values)
            {
                var state = amqp.GetState();
                if (state == AMQP.State.Running || state == AMQP.State.Paused)
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
                case "F":
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
        if (Regex.IsMatch(cmd, @"A|\d*D|\d*P|R|Q|F"))
        {
            inputPaused = true;
            switch (cmd)
            {
                case "A":
                    CreateWorkers(1);
                    break;
                case "D":
                    StopWorker();
                    break;
                case "R":
                    RestartRunningWorkers();
                    if (!isWorkersChecking) checkWorkersTimer.Change(0, workersCheckPeriod);
                    break;
                case "F":
                    if (!isWorkersChecking) checkWorkersTimer.Change(0, workersCheckPeriod);
                    break;
                case "P":
                    TogglePausedForAllWorkers();
                    break;
                case "Q":
                    checkWorkersTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    StopAllWorkers();
                    checkWorkersHandle.Set();
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
        if (!Monitor.TryEnter(lockOn, 100))
        {
            dbg.fa("Print() TryEnter failed");
            return;
        }
        output.Clear();
        string tmpl = "{0,3} {1,-14} {2,4} {3,5} {4,3} {5,4} {6,-8} {7,2} {8,-7} {9,-8} {10,-9} {11,6} {12,-19} {13,-19} {0,3}";
        string head = string.Format(
            tmpl,
            "no",
            "startTime",
            "aMsg",
            "axMsg",
            "aEr",
            "axEr",
            "aState",
            "aP",
            "axState",
            "axReqSt",
            "reqStart",
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
        bool isWaitingWorkersExists = false;
        DateTime dtNow = DateTime.Now;
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
                dynamic axInfo = axcon.GetInfo();
                AMQP.State amqpState = amqp.GetState();
                AxCon.State axconState = axcon.GetState();
                AxCon.RequestState axRequestState = axcon.GetRequestState();
                string amqpStateStr = (amqp.GetAsyncInitTimedOut() ? "!" : "") + amqpState.ToString();
                string axconStateStr = (axcon.GetAsyncInitTimedOut() ? "!" : "") + axconState.ToString();
                string axconReqStateStr = (axRequestState == AxCon.RequestState.NotApplicable) ? "" : axRequestState.ToString();
                double current_req_duration = axInfo["lastRequestStarttime"] != default(DateTime)
                    ? System.Math.Round((dtNow - axInfo["lastRequestStarttime"]).TotalMilliseconds/1000, 1)
                    : 0;
                string method = axRequestState == AxCon.RequestState.Request ? axInfo["lastMethod"].Replace("cmpECommerce", "").Trim('_') : "";
                string longestMethod = axInfo["longestMethod"].Replace("cmpECommerce", "").Trim('_');
                output.AppendLine(string.Format(
                    tmpl,
                    amqp.workerId,
                    amqp.startTime.ToString("MM-dd HH:mm:ss"),
                    amqp.msgCount,
                    axInfo["msgCount"],
                    amqp.errorCount,
                    axInfo["errorCount"],
                    amqpStateStr.Length <= 8 ? amqpStateStr : amqpStateStr.Substring(0, 8),
                    amqp.IsProcessing() ? "y" : "",
                    axconStateStr.Length <= 7 ? axconStateStr : axconStateStr.Substring(0, 7),
                    axconReqStateStr.Length <= 7 ? axconReqStateStr : axconReqStateStr.Substring(0, 7),
                    axInfo["lastRequestStarttime"] != default(DateTime) ? axInfo["lastRequestStarttime"].ToString("HH:mm:ss") : "",
                    axRequestState == AxCon.RequestState.Request && current_req_duration > 0 ? current_req_duration.ToString("0.0") : "",
                    method.Length <= 19 ? method : method.Substring(0, 19),
                    longestMethod.Length <= 19 ? longestMethod : longestMethod.Substring(0, 19)
                ));
                count++;
                if (amqpState != AMQP.State.Running && amqpState != AMQP.State.Paused && amqpState != AMQP.State.Stopped)
                {
                    isWaitingWorkersExists = true;
                }
            }
            objCount = string.Format("a={0} ax={1} w={2}", amqpDic.Count, axconDic.Count, workersCount);
            SaveStat();
            total = CalcTotalStat();
        }
        finally
        {
            Monitor.Exit(lockOn);
        }
        output.AppendLine(string.Format(
            "{0} {1}",
            isWaitingWorkersExists ? "Init/start/stop scheduled... " : "",
            IsAsyncQueueRunning() ? "Async queue running..." : ""
        ));
        output.AppendLine(string.Format(
            "Summary: amqpMsg={0} axMsg={1} amqpErr={2} axErr={3} workers={4} {8} heap={5:0.0}MB private={6:0.0}MB msgInQueue={7}",
            total["amqpMsgCount"],
            total["axMsgCount"],
            total["amqpErrorCount"],
            total["axErrorCount"],
            count,
            System.Math.Round(Convert.ToSingle(GC.GetTotalMemory(false)) / 1024 / 1024, 1),
            System.Math.Round(Convert.ToSingle(cur_proc.PrivateMemorySize64) / 1024 / 1024, 1),
            AMQP.msgInQueue.ToString(),
            objCount
        ));
        output.AppendLine(string.Format(
            "Config: workersCheckPeriod={0}s !amqpInitTimeout={1}s !axInitTimeout={2}s !axRequestTimeout={3}s startupWorkersCount={4}",
            workersCheckPeriod / 1000.0,
            amqpInitTimeout / 1000.0,
            axconInitTimeout / 1000.0,
            axconRequestTimeout / 1000.0,
            startupWorkersCount
        ));
        output.AppendLine(string.Format(
            "Refresh period: {0}s, running: {1:d\\.hh\\:mm\\:ss}",
            System.Math.Round(updateScreenPeriod / 1000.0, 1),
            (dtNow - startTime))
        );
        output.AppendLine("\n?: a: start new worker, [id]d: stop worker [by id], Ctrl-r: restart all workers, Ctrl-q: stop all workers and exit");
        output.AppendLine("?: <id>p: pause/resume worker by <id>, Ctrl-p: pause/resume all workers, <id>k: kill worker by id (not yet implemented)");
        output.AppendLine(string.Format(
            "?: f: force workers check {0}",
            nextWorkersCheck != default(DateTime) ? "(" + (nextWorkersCheck - dtNow).TotalSeconds.ToString("0") + ")": ""
        ));
        output.AppendLine("Space: pause screen update, Ctrl-C: force exit");
        if (!Monitor.TryEnter(lockOn, 200))
        {
            dbg.fa("Print() TryEnter failed 2");
            return;
        }
        else
        {
            Console.Clear();
            Console.Write(output.ToString());
            Console.Write("{0}", userInput);
            Monitor.Exit(lockOn);
        }
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
