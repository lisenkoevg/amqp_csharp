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
    private int workersCheckPeriod = 30000;
    private bool axconUseClassPool = true;
    private int startupWorkersCount;
    private readonly Process cur_proc = Process.GetCurrentProcess();
    private readonly DateTime startTime = DateTime.Now;
    private readonly bool isConsoleAvailable;
    private Dictionary<int, AMQP> amqpDic = new Dictionary<int, AMQP>();
    private Dictionary<int, AxCon> axconDic = new Dictionary<int, AxCon>();
    private Timer outputTimer;
    private Timer inputTimer;
    private Timer checkWorkersTimer;
    private AutoResetEvent outputAutoResetEvent = new AutoResetEvent(false);
    private AutoResetEvent inputAutoResetEvent = new AutoResetEvent(false);
    private AutoResetEvent checkWorkersAutoResetEvent = new AutoResetEvent(true);
    private int workersCount = 0;
    private bool outputPaused = false;
    private bool inputPaused = false;
    private bool isWorkersRestarting = false;
    private bool exitScheduled = false;
    private string userInput = "";
    private string infoMsg = "";
    private int updateScreenPeriod = 500;
    private int inputPollPeriod = 100;
    private DateTime nextWorkersCheck = default(DateTime);
    private Dictionary<int,Dictionary<string,int>> workersStatistics = new Dictionary<int,Dictionary<string,int>>();
    private Task asyncTaskChainHead = new Task(()=>{});
    private Task asyncTaskChainTail = null;
    private object lockOn = new object();
    private StringBuilder output = new StringBuilder();

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
        outputAutoResetEvent.WaitOne();
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
        if (conf.ContainsKey("axconUseClassPool") && Int32.TryParse(conf["axconUseClassPool"], out parsedValue))
        {
            axconUseClassPool = parsedValue != 0;
        }
    }

    private void Work(object count)
    {
        CreateWorkers((int)count);
        CheckWorkers();
        int secondCheckTimeout = 10000;
        nextWorkersCheck = DateTime.Now.AddMilliseconds(secondCheckTimeout);
        checkWorkersTimer = new Timer(
            (obj) => {
                checkWorkersAutoResetEvent.WaitOne();
                nextWorkersCheck = default(DateTime);
                CheckWorkers();
                nextWorkersCheck = DateTime.Now.AddMilliseconds(workersCheckPeriod);
                checkWorkersAutoResetEvent.Set();
            },
            null,
            secondCheckTimeout,
            workersCheckPeriod
        );
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
        inputAutoResetEvent.WaitOne();
        try { inputTimer.Dispose(); } catch {};
        try { checkWorkersTimer.Dispose(); } catch {};
    }

    private void CreateWorkers(int count)
    {
        for (int i = 0; i < count; i++)
        {
            if (GetWorkersCount() >= maxWorkersCount || exitScheduled) break;
            CreateWorker();
        }
    }

    private void CreateWorker()
    {
        lock (lockOn)
        {
            int workerId = GetWorkerId();
            var amqp = new AMQP(workerId);
            var axcon = new AxCon(workerId);
            axcon.useClassPool = axconUseClassPool;
            amqpDic.Add(workerId, amqp);
            axconDic.Add(workerId, axcon);
            workersCount++;
        }
    }

    private void CheckWorkers()
    {
        List<int> list;
        lock (lockOn)
        {
            list = amqpDic.Keys.ToList();
        }
        list.Sort();
        if (IsAcynTaskChainCompleted())
            InitAsyncTaskChain();
        {
        }
        foreach (int workerId in list)
        {
            CheckWorker(workerId);
        }
        if (IsAsyncTaskChainReady())
        {
            asyncTaskChainHead.Start();
        }
        if (exitScheduled && list.Count == 0)
        {
            inputAutoResetEvent.Set();
            outputAutoResetEvent.Set();
        }
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
        bool axconAsyncRequestTimedOut;
        bool isProcessing;
        bool isAsyncTaskChainRunning;
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
                axconAsyncRequestTimedOut = axcon.GetAsyncRequestTimedOut();
                isProcessing = amqp.IsProcessing();
                requestState = axcon.GetRequestState();
                isAsyncTaskChainRunning = IsAsyncTaskChainRunning();
            }
            else
            {
                return;
            }
        }

        if (exitScheduled
            && !(isAsyncTaskChainRunning && (amqpState == AMQP.State.Init || amqpState == AMQP.State.Connect))
            && amqpState != AMQP.State.StopPend && amqpState != AMQP.State.Stopped)
        {
            amqp.StopPend();
        }
        if (amqpState == AMQP.State.StopPend || amqpState == AMQP.State.Stopped)
        {
            if (!isProcessing && requestState != AxCon.RequestState.Request)
            {
                if (axconState == AxCon.State.Ready)
                {
                    ScheduleFinWorkerAxCon(axcon);
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
        if (amqpState == AMQP.State.Init)
        {
            ScheduleInitWorkerAMQP(amqp);
        }
        if (axconState == AxCon.State.Init)
        {
            if (amqpState != AMQP.State.StopPend && amqpState != AMQP.State.Stopped)
                ScheduleInitWorkerAxCon(axcon);
        }
        if (amqpState == AMQP.State.Ready && axconState == AxCon.State.Ready && !exitScheduled)
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
            ScheduleFinWorkerAxCon(axcon);
        }
        if (axconState == AxCon.State.InitError)
        {
            ScheduleFinWorkerAxCon(axcon);
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
        if (amqpState == AMQP.State.Error)
        {
            amqp.SetInitState();
        }
        if (amqpState == AMQP.State.ForcePaused && (requestState == AxCon.RequestState.ReqErr || axconAsyncRequestTimedOut))
        {
            ScheduleFinWorkerAxCon(axcon);
            lock (lockOn)
            {
                axcon.SetRequestState(AxCon.RequestState.NotApplicable);
                axcon.SetAsyncRequestTimedOut(false);
            }
        }
        if (amqpState == AMQP.State.ForcePaused && axconState == AxCon.State.Ready
            && requestState != AxCon.RequestState.ReqErr && !axconAsyncRequestTimedOut)
        {
            amqp.ForceResume();
        }
    }

    private bool IsAcynTaskChainCompleted()
    {
        return asyncTaskChainTail != null && asyncTaskChainTail.Status == TaskStatus.RanToCompletion;
    }

    private void InitAsyncTaskChain()
    {
        asyncTaskChainHead.Dispose();
        asyncTaskChainTail.Dispose();
        asyncTaskChainHead = new Task(()=>{});
        asyncTaskChainTail = null;
    }

    private bool IsAsyncTaskChainReady()
    {
        return asyncTaskChainHead.Status == TaskStatus.Created && asyncTaskChainTail != null;
    }

    private bool IsAsyncTaskChainRunning()
    {
        return asyncTaskChainTail != null
            && asyncTaskChainHead.Status == TaskStatus.RanToCompletion
            && asyncTaskChainTail.Status != TaskStatus.RanToCompletion;
    }

    private void ScheduleInitWorkerAMQP(AMQP amqp)
    {
        if (!IsAsyncTaskChainRunning())
        {
            Task nextTask = asyncTaskChainTail != null ? asyncTaskChainTail : asyncTaskChainHead;
            asyncTaskChainTail = nextTask.ContinueWith(
                (t) => {
                    amqp.SetAsyncInitTimedOut(false);
                    Task task = Task.Factory.StartNew((Action)amqp.Init);
                    bool waitSuccess = task.Wait(amqpInitTimeout);
                    amqp.SetAsyncInitTimedOut(!waitSuccess);
                    task.Wait();
                    if (!waitSuccess)
                    {
                        amqp.log("Connection timeout workerId=" + amqp.workerId, "error");
                    }
                }
            );
        }
    }

    private void ScheduleInitWorkerAxCon(AxCon axcon)
    {
        ScheduleInitOrFinWorkerAxCon(axcon, axcon.Init, AxCon.State.Login);
    }

    private void ScheduleFinWorkerAxCon(AxCon axcon)
    {
        ScheduleInitOrFinWorkerAxCon(axcon, axcon.Fin, AxCon.State.Logoff);
    }

    private void ScheduleInitOrFinWorkerAxCon(AxCon axcon, Action action, AxCon.State stateBefore)
    {
        if (!IsAsyncTaskChainRunning())
        {
            Task nextTask = asyncTaskChainTail != null ? asyncTaskChainTail : asyncTaskChainHead;
            asyncTaskChainTail = nextTask.ContinueWith(
                (t) => {
                    axcon.SetAsyncInitTimedOut(false);
                    Task task = Task.Factory.StartNew(action);
                    bool waitSuccess = task.Wait(axconInitTimeout);

                    if (axcon.GetState() == stateBefore)
                    {
                        axcon.SetAsyncInitTimedOut(!waitSuccess);
                    }
                    else
                    {
                        // Low probability situation when operation finished exactly in timeout ms time period
                    }
                    task.Wait();
                    if (!waitSuccess)
                    {
                        axcon.log(action.Method + " timeout workerId=" + axcon.workerId, "error");
                    }
                }
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
        AxCon axcon = axconDic[amqp.workerId];

        axcon.lastRequestStarttime = DateTime.Now;
        axcon.lastMethod = Util.RemoveVowels(method);

        var response = new Dictionary<string,object>();
        axcon.SetAsyncRequestTimedOut(false);

        Stopwatch stopWatch = Stopwatch.StartNew();
        response = axcon.request(method, prms, id);
        stopWatch.Stop();
        var requestState = axcon.GetRequestState();

        //TODO async axcon.request
        // Task<Dictionary<string,object>> task = Task.Factory.StartNew(()=>{
            // return axcon.request(method, prms, id);
        // });
        // bool waitSuccess = task.Wait(axconRequestTimeout);
        // axcon.SetAsyncRequestTimedOut(!waitSuccess);
        bool waitSuccess = true;

        response["elapsed"] = stopWatch.ElapsedMilliseconds;
        if (waitSuccess)
        {
            // response = task.Result;
            if (requestState == AxCon.RequestState.ReqErr)
            {
                amqp.ForcePause();
            }
            else
            {
                TimeSpan last_method_duration = stopWatch.Elapsed;
                if (axcon.longestMethod == "" || last_method_duration > axcon.longestMethodDuration)
                {
                    axcon.longestMethod = axcon.lastMethod;
                    axcon.longestMethodDuration = last_method_duration;
                }
            }
        }
        else
        {
            amqp.ForcePause();
            axcon.requestTimedOutCount++;
            axcon.log(
                new Dictionary<string, object>() {
                    {"method", method},
                    {"params", prms},
                    {"id", id}
                },
                "request_timedout",
                true
            );
            response = new Dictionary<string, object>() {
                {"result", null},
                {"error", "server timed out"},
                {"id", id},
                {"elapsed", axconRequestTimeout}
            };
            axcon.log(response, "response", true);
            axcon.log(response, "", true, true);
        }

        string fileSuffix = waitSuccess ? "response" : "response_timedout_skipped";
        axcon.log(response, fileSuffix, true);
        if (waitSuccess)
            axcon.log(response, "", true, true);

        return response;
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
                if (state == AMQP.State.Running || state == AMQP.State.Paused)
                {
                    amqp.StopPend();
                    break;
                }
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
                    else
                    {
                        userInput += keyChar;
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
                        if (Regex.IsMatch(userInput.ToUpper(), @"\d*D|\d*P|RC"))
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
        if (Regex.IsMatch(cmd, @"A|\d*D|\d*P|R|Q|F|RC"))
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
                    checkWorkersTimer.Change(1000, workersCheckPeriod);
                    break;
                case "F":
                    checkWorkersTimer.Change(0, workersCheckPeriod);
                    break;
                case "P":
                    TogglePausedForAllWorkers();
                    break;
                case "Q":
                    if (!exitScheduled)
                    {
                        exitScheduled = true;
                        workersCheckPeriod = 5000;
                        checkWorkersTimer.Change(0, workersCheckPeriod);
                    }
                    break;
                case "RC":
                    infoMsg = AxCon.LoadConfig();
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
            inputPaused = false;
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
            return;
        }
        output.Clear();
        string tmpl = "{0,3} {1,-14} {2,6} {3,6} {4,3} {5,4} {6,7} {7,-10} {8,-7} {9,-8} {10,-8} {11,6} {12,-14} {13,-18} {0,3}";
        string head = string.Format(
            tmpl,
            "no",
            "startTime",
            "aMsg",
            "axMsg",
            "aEr",
            "axEr",
            "axRE/TO",
            "aState",
            "axState",
            "axReqSt",
            "reqStart",
            "reqDur",
            "request",
            "longest"
        );
        output.AppendLine(head);
        output.AppendLine(string.Format("{0}", "".PadLeft(head.Length, '=')));

        Dictionary<string,int> total;
        List<int> list;
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
                AxCon.RequestState axconRequestState = axcon.GetRequestState();
                string amqpStateStr = (amqp.GetAsyncInitTimedOut() ? "!" : "") + amqpState.ToString() + (amqp.IsProcessing() ? "+" : "");
                string axconStateStr = (axcon.GetAsyncInitTimedOut() ? "!" : "") + axconState.ToString();
                string axconRequestStateStr = (axconRequestState == AxCon.RequestState.NotApplicable) ? "" : axconRequestState.ToString();
                axconRequestStateStr = (axcon.GetAsyncRequestTimedOut() ? "!" : "") + axconRequestStateStr;
                double current_req_duration = axInfo["lastRequestStarttime"] != default(DateTime)
                    ? System.Math.Round((dtNow - axInfo["lastRequestStarttime"]).TotalMilliseconds/1000, 1)
                    : 0;
                string method = axconRequestState == AxCon.RequestState.Request ? axInfo["lastMethod"].Replace("cmpECommerce", "").Trim('_') : "";
                string longestMethod = axInfo["longestMethod"].Replace("cmpECommerce", "").Trim('_');
                longestMethod = longestMethod.Length <= 14 ? longestMethod : longestMethod.Substring(0, 14);
                longestMethod += (axInfo["longestMethodDuration"] != default(TimeSpan)
                    ? "|" + (axInfo["longestMethodDuration"].TotalSeconds < 99
                        ? axInfo["longestMethodDuration"].TotalSeconds.ToString("0")
                        : "99")
                    : "" );
                output.AppendLine(string.Format(
                    tmpl,
                    amqp.workerId,
                    amqp.startTime.ToString("MM-dd HH:mm:ss"),
                    amqp.msgCount,
                    (amqp.msgCount != axInfo["msgCount"] ? "*" : "") + axInfo["msgCount"].ToString(),
                    amqp.errorCount,
                    axInfo["errorCount"],
                    axInfo["requestErrorCount"].ToString() + "/" + axInfo["requestTimedOutCount"].ToString(),
                    amqpStateStr.Length <= 10 ? amqpStateStr : amqpStateStr.Substring(0, 10),
                    axconStateStr.Length <= 7 ? axconStateStr : axconStateStr.Substring(0, 7),
                    axconRequestStateStr.Length <= 8 ? axconRequestStateStr : axconRequestStateStr.Substring(0, 8),
                    axInfo["lastRequestStarttime"] != default(DateTime) ? axInfo["lastRequestStarttime"].ToString("HH:mm:ss") : "",
                    axconRequestState == AxCon.RequestState.Request && current_req_duration > 0 ? current_req_duration.ToString("0.0") : "",
                    method.Length <= 14 ? method : method.Substring(0, 14),
                    longestMethod
                    
                ));
                if (amqpState != AMQP.State.Running && amqpState != AMQP.State.Paused)
                {
                    isWaitingWorkersExists = true;
                }
            }
            SaveStat();
            total = CalcTotalStatistics();
        }
        finally
        {
            Monitor.Exit(lockOn);
        }
        output.AppendLine(string.Format(
            "{0} {1} {2}",
            exitScheduled ? "Exiting..." : "",
            isWaitingWorkersExists ? "Init/start/stop scheduled... " : "",
            IsAsyncTaskChainRunning() ? "Async task running..." : ""
        ).Trim());
        output.AppendLine(string.Format(
            "Summary: workers={0} amqpMsg={1} axMsg={2} amqpConnErr={3} axConnErr={4} axRequestError={5} axReqTimedOut={6} msgInQueue<{7}>~{8}",
            workersCount,
            total["amqpMsgCount"],
            total["axMsgCount"],
            total["amqpErrorCount"],
            total["axErrorCount"],
            total["axRequestErrorCount"],
            total["axRequestTimedOutCount"],
            AMQP.queue,
            AMQP.msgInQueue
        ));
        output.AppendLine(string.Format(
            "Config: workersCheckPeriod={0}s !amqpInitTimeout={1}s !axInitTimeout={2}s !axRequestTimeout={3}s startupWorkersCount={4} useClassPool={5}\n YAMLtimestamp={6}",
            workersCheckPeriod / 1000.0,
            amqpInitTimeout / 1000.0,
            axconInitTimeout / 1000.0,
            axconRequestTimeout / 1000.0,
            startupWorkersCount,
            axconUseClassPool ? "yes" : "no",
            AxCon.config["lastModified"].ToString("MM-dd HH:mm:ss")
        ));
        PrintToLog(output.ToString(), dtNow);
        output.AppendLine(string.Format(
            "Screen update period: {0}s running: {1:d\\.hh\\:mm\\:ss} heap={2:0.0}MB", //private={3:0.0}MB threads={4}
            System.Math.Round(updateScreenPeriod / 1000.0, 1),
            (dtNow - startTime),
            System.Math.Round(Convert.ToSingle(GC.GetTotalMemory(false)) / 1024 / 1024, 1)
            // System.Math.Round(Convert.ToSingle(cur_proc.PrivateMemorySize64) / 1024 / 1024, 1)
            // cur_proc.Threads.Count
        ));
        output.AppendLine("\n?: a: start new worker, d - stop one running worker, <id>d: stop worker by <id>");
        output.AppendLine("?: Ctrl-r: restart all workers, Ctrl-q: stop all workers and exit");
        output.AppendLine("?: <id>p: pause/resume worker by <id>, Ctrl-p: pause/resume all workers");
        output.AppendLine(string.Format(
            "?: f: force workers check {0}, rc: reload config *.yaml",
            nextWorkersCheck != default(DateTime) ? "(" + (nextWorkersCheck - dtNow).TotalSeconds.ToString("0") + ")": ""
        ));
        output.AppendLine("Space: pause screen update, Ctrl-C: force exit");
        if (infoMsg != "")
        {
            output.AppendLine(string.Format("{0}", "".PadLeft(head.Length, '=')));
            output.AppendLine(infoMsg);
        }
        if (Monitor.TryEnter(lockOn, 200))
        {
            try
            {
                Console.Clear();
                Console.Write(output.ToString());
                Console.Write("{0}", userInput);
            }
            catch (Exception e)
            {
                dbg.fa(e);
            }
            finally
            {
                Monitor.Exit(lockOn);
            }
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
            dynamic axinfo = axcon.GetInfo();
            if (!workersStatistics.ContainsKey(workerId))
            {
                workersStatistics.Add(
                    workerId,
                    new Dictionary<string,int>(){
                        {"amqpMsgCount", 0},
                        {"axMsgCount", 0},
                        {"amqpErrorCount", 0},
                        {"axErrorCount", 0},
                        {"axRequestErrorCount", 0},
                        {"axRequestTimedOutCount", 0}
                    }
                 );
            }
            workersStatistics[workerId]["amqpMsgCount"] = amqp.msgCount;
            workersStatistics[workerId]["axMsgCount"] = axinfo["msgCount"];
            workersStatistics[workerId]["amqpErrorCount"] = amqp.errorCount;
            workersStatistics[workerId]["axErrorCount"] = axinfo["errorCount"];
            workersStatistics[workerId]["axRequestErrorCount"] = axinfo["requestErrorCount"];
            workersStatistics[workerId]["axRequestTimedOutCount"] = axinfo["requestTimedOutCount"];
        }
    }

    private Dictionary<string,int> CalcTotalStatistics()
    {
        Dictionary<string,int> result =
            new Dictionary<string,int>(){
                {"amqpMsgCount", 0},
                {"axMsgCount", 0},
                {"amqpErrorCount", 0},
                {"axErrorCount", 0},
                {"axRequestErrorCount", 0},
                {"axRequestTimedOutCount", 0}
            };
        foreach (var i in workersStatistics)
        {
            result["amqpMsgCount"] += i.Value["amqpMsgCount"];
            result["axMsgCount"] += i.Value["axMsgCount"];
            result["amqpErrorCount"] += i.Value["amqpErrorCount"];
            result["axErrorCount"] += i.Value["axErrorCount"];
            result["axRequestErrorCount"] += i.Value["axRequestErrorCount"];
            result["axRequestTimedOutCount"] += i.Value["axRequestTimedOutCount"];
        }
        return result;
    }

    private void PrintToLog(string s, DateTime dtNow)
    {
        if (dtNow.Second < 2 || (dtNow.Second > 29 && dtNow.Second < 32))
        {
            Directory.CreateDirectory(logDir + "\\print");
            var file_name = dtNow.ToString("yyyyMMddHHmmss") + ".log";
            StreamWriter writer = null;
            try
            {
                writer = new StreamWriter(logDir + "\\print\\" + file_name, false);
                writer.WriteLine(s);
            }
            finally
            {
                writer.Dispose();
            }
        }
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
