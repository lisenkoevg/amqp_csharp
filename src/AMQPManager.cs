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

public partial class AMQPManager
{
    public enum State {Running, UserStop, UserRestart, ErrorStop, SupervisorStop, Crash}
    private State state = State.Running;
    private static string configFile = @"config\AMQPManager.yaml";
    public static string logDir = Path.GetDirectoryName(configFile) + "\\" + "..\\log";
    public static int managersCount = 1;
    private static string cmdConfigFile;
    private static int maxWorkersCount = 30;
    private static int amqpInitTimeout = 15000;
    private static int axconInitTimeout = 30000;
    private static int axconRequestTimeout = 60000;
    private static int workersCheckPeriod = 15000;
    private static bool axconUseClassPool = false;
    private static int startupWorkersCount = 4;

    private readonly Process currentProcess = Process.GetCurrentProcess();
    private Process parentProcess;
    private readonly DateTime startTime = DateTime.Now;

    private Dictionary<int, AMQP> amqpDic = new Dictionary<int, AMQP>();
    private Dictionary<int, AxCon> axconDic = new Dictionary<int, AxCon>();
    private Timer outputTimer;
    private Timer inputTimer;
    private Timer checkWorkersTimer;
    private AutoResetEvent workAutoResetEvent = new AutoResetEvent(false);
    private AutoResetEvent checkWorkersAutoResetEvent = new AutoResetEvent(true);
    private AutoResetEvent printAutoResetEvent = new AutoResetEvent(true);
    private int _workerId = 0;
    private int workersCount = 0;
    private bool outputPaused = false;
    private bool inputPaused = false;
    private bool exitScheduled = false;
    private bool isBusinessConnectorInstanceInvalid;
    private DateTime nextWorkersCheck = default(DateTime);
    private Dictionary<int,Dictionary<string,int>> workersStatistics = new Dictionary<int,Dictionary<string,int>>();
    private Task asyncTaskChainHead = new Task(()=>{});
    private Task asyncTaskChainTail = null;
    private object lockOn = new object();
    private string userInput = "";
    private string infoMsg = "";
    private string lastPrint = "";
    private int updateScreenPeriod = 500;
    private int inputPollPeriod = 100;
    private int width = 0;
    private int height = 0;
    private Logger logger = new Logger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);
    
    public static void ShowHelp()
    {
        String exename = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase).Replace("file:\\", "");
        string msg = string.Format("Command-line options:\n{0} [-config=<path to config>]", exename);
        msg += string.Format("\nDefault config \"{0}\"", configFile);
        if (Supervisor.isConsoleAvailable)
            Console.WriteLine(msg);
    }

    public static void Configure()
    {
        dynamic conf = LoadConfig();
        if (conf == null)
            return;
        int parsedValue = 0;
        if (conf.ContainsKey("maxWorkersCount") && Int32.TryParse(conf["maxWorkersCount"], out parsedValue))
        {
            maxWorkersCount = parsedValue;
        }
        if (conf.ContainsKey("managersCount") && Int32.TryParse(conf["managersCount"], out parsedValue))
        {
            if (parsedValue > 0 && parsedValue < 30)
            {
                managersCount = parsedValue;
            }
        }
        if (conf.ContainsKey("startupWorkersCount") && Int32.TryParse(conf["startupWorkersCount"], out parsedValue))
        {
            if (parsedValue > 0 && parsedValue <= maxWorkersCount)
            {
                startupWorkersCount = parsedValue;
            }
        }
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
        if (conf.ContainsKey("logDir") && conf["logDir"] != null)
        {
            conf["logDir"] = conf["logDir"].Trim();
            if (conf["logDir"].Length > 0)
            {
                logDir = Path.IsPathRooted(conf["logDir"]) ? conf["logDir"] : Path.GetDirectoryName(configFile) + "\\" + conf["logDir"];
            }
        }
    }

    public static object LoadConfig()
    {
        object result = null;
        cmdConfigFile = Supervisor.GetArg("config");
        if (cmdConfigFile != null)
        {
            configFile = cmdConfigFile;
        }
        if (File.Exists(configFile))
        {
            try
            {
                result = ConfigLoader.LoadFile(configFile);
            }
            catch (Exception e)
            {
                try {Console.Error.WriteLine(e.Message);} catch {}
                Environment.Exit(1);
            }
        }
        else
        {
            if (cmdConfigFile != null)
            {
                try {Console.Error.WriteLine("Config file '{0}' not found.", configFile);} catch {}
                Environment.Exit(1);
            }
            else
            {
                CreateConfigFile();
            }
        }
        return result;
    }

    public static void CreateConfigFile()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(configFile));
            using (var sw = new StreamWriter(configFile))
            {
                sw.WriteLine("# timeouts in milliseconds");
                sw.WriteLine("amqpInitTimeout: {0}", amqpInitTimeout);
                sw.WriteLine("axconInitTimeout: {0}", axconInitTimeout);
                sw.WriteLine("axconRequestTimeout: {0}", axconRequestTimeout);
                sw.WriteLine("startupWorkersCount: {0}", startupWorkersCount);
                sw.WriteLine("managersCount: {0}", managersCount);
                sw.WriteLine("maxWorkersCount: {0}", maxWorkersCount);
                sw.WriteLine("# 1/0");
                sw.WriteLine("axconUseClassPool: {0}", axconUseClassPool ? "1" : "0");
                sw.WriteLine("# logDir relative to config file or absolute if started with \\ or <letter>:");
                sw.WriteLine("logDir: {0}", logDir.Substring(Path.GetDirectoryName(configFile).Length + 1));
            }
        }
        catch (Exception e)
        {
            try { Console.Error.WriteLine(e); } catch {};
            Environment.Exit(1);
        }
    }

    public static void PrepareLogDir()
    {
        string testFile = logDir + "\\" + Path.GetRandomFileName();
        try
        {
            Directory.CreateDirectory(logDir);
            using (var sw = new StreamWriter(testFile))
            {
                sw.WriteLine("Check if writable");
            }
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Can't create or write to log dir [{0}]\n{1}", logDir, e);
            Environment.Exit(1);
        }
        finally
        {
            try { File.Delete(testFile); } catch {}
        }
    }

    public static Process SpawnNewInstance(int id, int parentProcessId, string exeName, bool redirectError = true)
    {
        Process result;
        string prefix = "Exception";
        var psi = new ProcessStartInfo();
        psi.FileName = "cmd.exe";
        string config = "";
        if (cmdConfigFile != null)
        {
            config = cmdConfigFile.IndexOf(' ') != -1 ? "\"" + cmdConfigFile + "\"" : cmdConfigFile;
            config = " -config=" + config;
        }
        psi.Arguments = string.Format("/c {0}{1} -parentPID={2}", exeName, config, parentProcessId);
        if (redirectError)
        {
            psi.Arguments += string.Format(" 2>>\"{0}\\{1}{2}.{3}.log\"", logDir, prefix, parentProcessId, id);
        }
        result = Process.Start(psi);
        result = Supervisor.WaitForChildProcess(result.Id, Path.GetFileName(exeName));
        ClearEmptyExceptionLogs(prefix);
        ClearEmptyExceptionLogs("UnhandledException");
        return result;
    }
    
    private static void ClearEmptyExceptionLogs(string prefix)
    {
        string[] files = new string[0];
        try { files = Directory.GetFiles(Environment.CurrentDirectory + "\\" + logDir, prefix + "*.log"); } catch {}
        foreach (var f in files)
        {
            try
            {
                long length = new System.IO.FileInfo(f).Length;
                if (length < 5 && Regex.IsMatch(f, prefix + @"[.\d]{1,10}\.log$"))
                    File.Delete(f);
            }
            catch {}
        }
    }
    
    public AMQPManager()
    {
        string parentPID = Supervisor.GetArg("parentPID");
        try
        {
            parentProcess = Process.GetProcessById(Convert.ToInt32(parentPID));
        }
        catch (Exception e)
        {
            string msg = string.Format("Get parent process error parentPID={0}\n{1}", parentPID, e);
            infoMsg += msg;
            logger.Log(msg);
        }
        if (Supervisor.isConsoleAvailable)
        {
            Console.Title = System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name;
            Console.CancelKeyPress += new ConsoleCancelEventHandler(OnConsoleCancel);
        }
        checkWorkersTimer = new Timer((obj) => { CheckWorkers(); });
        outputTimer = new Timer((obj) => { Print(); });
        inputTimer = new Timer((obj) => { HandleInput(); });
        
        Work(startupWorkersCount);
    }

    private void Work(int count)
    {
        StartPipeClient();
        CreateWorkers(count);
        CheckWorkers();
        int secondCheckTimeout = 7000;
        nextWorkersCheck = DateTime.Now.AddMilliseconds(secondCheckTimeout);
        checkWorkersTimer.Change(secondCheckTimeout, workersCheckPeriod);
        inputTimer.Change(0, inputPollPeriod);
        outputTimer.Change(0, updateScreenPeriod);
        workAutoResetEvent.WaitOne();
        StopPipeClient();
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
            amqp.OnAxPrepareRequest += AxPrepareRequest;
            amqp.OnAxExecuteRequest += AxExecuteRequest;
            var axcon = new AxCon(workerId);
            axcon.OnProcessCorruptedStateException += ErrorScheduleApplicationRestart;
            axcon.useClassPool = axconUseClassPool;
            amqpDic.Add(workerId, amqp);
            axconDic.Add(workerId, axcon);
            workersCount++;
        }
    }

    private void CheckWorkers()
    {
        checkWorkersAutoResetEvent.WaitOne();
        nextWorkersCheck = default(DateTime);
        
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
        isBusinessConnectorInstanceInvalid = true;
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
            workAutoResetEvent.Set();
        }
        if (isBusinessConnectorInstanceInvalid && list.Count > 0)
        {
            ErrorScheduleApplicationRestart("BusinessConnectorInstanceInvalid");
        }
        if (parentProcess == null || parentProcess.HasExited)
        {
            ScheduleApplicationExit(false, "Parent is gone");
        }
        nextWorkersCheck = DateTime.Now.AddMilliseconds(workersCheckPeriod);
        checkWorkersAutoResetEvent.Set();    
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
        if (!exitScheduled)
        {
            if (amqpState == AMQP.State.Init)
            {
                ScheduleInitWorkerAMQP(amqp);
            }
            if (axconState == AxCon.State.Init)
            {
                if (amqpState != AMQP.State.StopPend && amqpState != AMQP.State.Stopped)
                    ScheduleInitWorkerAxCon(axcon);
            }
            if (amqpState == AMQP.State.Ready && axconState == AxCon.State.Ready)
            {
                StartWorker(amqp);
            }
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
        if (requestState == AxCon.RequestState.ReqErr || requestState == AxCon.RequestState.PrepErr || axconAsyncRequestTimedOut)
        {
            if (ScheduleFinWorkerAxCon(axcon))
            {
                lock (lockOn)
                {
                    axcon.SetRequestState(AxCon.RequestState.NotApplicable);
                    axcon.SetAsyncRequestTimedOut(false);
                }
            }
        }
        if (amqpState == AMQP.State.ForcePaused && axconState == AxCon.State.Ready && requestState == AxCon.RequestState.WaitReq)
        {
            amqp.ForceResume();
        }
        if (amqpState == AMQP.State.StopPend || amqpState == AMQP.State.Stopped)
        {
            if (!isProcessing && requestState != AxCon.RequestState.Request && requestState != AxCon.RequestState.Prepare)
            {
                if (axconState == AxCon.State.Ready)
                {
                    ScheduleFinWorkerAxCon(axcon);
                }
                else
                {
                    if (amqpState == AMQP.State.Stopped && axconState != AxCon.State.Logoff)
                    {
                        amqp.SetAsyncInitTimedOut(false);
                        axcon.SetAsyncInitTimedOut(false);
                        DeleteWorker(amqp);
                    }
                }
            }
        }
        isBusinessConnectorInstanceInvalid = isBusinessConnectorInstanceInvalid && axcon.isBusinessConnectorInstanceInvalid;
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
                    Task task = Task.Factory.StartNew((Action)amqp.Init, TaskCreationOptions.LongRunning);
                    bool waitSuccess = task.Wait(amqpInitTimeout);
                    amqp.SetAsyncInitTimedOut(!waitSuccess);
                    task.Wait();
                    if (!waitSuccess)
                    {
                        AMQP.logger.Log(amqp.workerId, "Connection timeout", "error");
                    }
                }
            );
        }
    }

    private void ScheduleInitWorkerAxCon(AxCon axcon)
    {
        ScheduleInitOrFinWorkerAxCon(axcon, axcon.Init, AxCon.State.Login);
    }

    private bool ScheduleFinWorkerAxCon(AxCon axcon)
    {
        return ScheduleInitOrFinWorkerAxCon(axcon, axcon.Fin, AxCon.State.Logoff);
    }

    private bool ScheduleInitOrFinWorkerAxCon(AxCon axcon, Action action, AxCon.State stateBefore)
    {
        if (!IsAsyncTaskChainRunning())
        {
            Task nextTask = asyncTaskChainTail != null ? asyncTaskChainTail : asyncTaskChainHead;
            asyncTaskChainTail = nextTask.ContinueWith(
                (t) => {
                    axcon.SetAsyncInitTimedOut(false);
                    Task task = Task.Factory.StartNew(action, TaskCreationOptions.LongRunning);
                    bool waitSuccess = task.Wait(axconInitTimeout);
                    if (!waitSuccess)
                    {
                        if (axcon.GetState() == stateBefore)
                        {
                            axcon.SetAsyncInitTimedOut(true);
                            axcon.errorCount++;
                        }
                        else
                        {
                        // Low probability situation when operation finished exactly in timeout ms time period
                        }
                    }
                    task.Wait();
                    if (!waitSuccess)
                    {
                        AxCon.logger.Log(axcon.workerId, action.Method.Name + " timeout", "error");
                    }
                }
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    private void StartWorker(AMQP amqp)
    {
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

    private AxCon.RequestState AxPrepareRequest(AMQP amqp, string method, Dictionary<string,object> prms, string id)
    {
        AxCon axcon = axconDic[amqp.workerId];
        axcon.keepAliveManualResetEvent.WaitOne();
        axcon.lastMethod = Util.RemoveVowels(method);
        axcon.lastRequestStarttime = DateTime.Now;
        axcon.stopwatch.Restart();

        axcon.SetAsyncRequestTimedOut(false);
        var requestState = axcon.PrepareRequest(method, prms, id);
        if (requestState == AxCon.RequestState.PrepErr)
        {
            amqp.ForcePause();
        }
        return requestState;
    }

    private Dictionary<string,object> AxExecuteRequest(AMQP amqp, string id)
    {
        AxCon axcon = axconDic[amqp.workerId];
        var response = new Dictionary<string,object>();
        axcon.SetAsyncRequestTimedOut(false);
        // response = axcon.ExecuteRequest();
        // bool waitSuccess = true;
        Task<Dictionary<string,object>> task = Task.Factory.StartNew(
            ()=> {
                return axcon.ExecuteRequest();
            },
            TaskCreationOptions.LongRunning
        );
        bool waitSuccess = task.Wait(axconRequestTimeout);
        var requestState = axcon.GetRequestState();
        if (!waitSuccess)
        {
            if (requestState == AxCon.RequestState.Request)
            {
                axcon.SetAsyncRequestTimedOut(true);
            }
            else
            {
                // Low probability situation when operation finished exactly in timeout ms time period
            }
        }
        axcon.stopwatch.Stop();
        response["elapsed"] = axcon.stopwatch.ElapsedMilliseconds;
        if (waitSuccess)
        {
            response = task.Result;
            if (requestState != AxCon.RequestState.ReqErr)
            {
                TimeSpan last_method_duration = axcon.stopwatch.Elapsed;
                if (axcon.longestMethod == "" || last_method_duration > axcon.longestMethodDuration)
                {
                    axcon.longestMethod = axcon.lastMethod;
                    axcon.longestMethodDuration = last_method_duration;
                }
            }
            else
            {
                amqp.ForcePause();
            }
        }
        else
        {
            amqp.ForcePause();
            axcon.requestTimedOutCount++;
            response = new Dictionary<string, object>() {
                {"result", null},
                {"error", new Dictionary<string,object>(){{"code", -32001}, {"message", "Server timed out"}}},
                {"id", id},
                {"elapsed", axcon.stopwatch.ElapsedMilliseconds}
            };
        }
        AxCon.logger.LogInJSON(axcon.workerId, response, "response");
        AxCon.logger.LogInJSON(axcon.workerId, response, "", true);
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
    
    private void UserScheduleApplicationExit(bool restart = false)
    {
        this.state = restart ? State.UserRestart : State.UserStop;
        ScheduleApplicationExit(restart, "");
    }
    
    private void ErrorScheduleApplicationRestart(string descr)
    {
        if (state != State.ErrorStop)
        {
            state = State.ErrorStop;
            ScheduleApplicationExit(true, descr);
        }
    }
    
    private void ScheduleApplicationExit(bool restart = false, string descr = "")
    {
        if (!exitScheduled)
        {
            exitScheduled = true;
            workersCheckPeriod = 2000;
            checkWorkersTimer.Change(0, workersCheckPeriod);
            SendOwnStateToSupervisor(descr);
        }
    }

    private void HandleInput()
    {
        if (!(Supervisor.isConsoleAvailable && Console.KeyAvailable) || inputPaused) return;
        ConsoleKeyInfo ki = Console.ReadKey(outputPaused || inputPaused);

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
                    UserScheduleApplicationExit(restart: true);
                    break;
                case "F":
                    checkWorkersTimer.Change(0, workersCheckPeriod);
                    break;
                case "P":
                    TogglePausedForAllWorkers();
                    break;
                case "Q":
                    UserScheduleApplicationExit();
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
        if (outputPaused) return;
        if (!Monitor.TryEnter(lockOn, 100))
        {
            return;
        }
        StringBuilder output = new StringBuilder();
        string tmpl = "{0,3} {1,-14} {2,6} {3,6} {4,3} {5,4} {6,7} {7,-10} {8,-7} {9,-8} {10,-8} {11,4} {12,-14} {13,-17} {14,4} {0,3}";
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
            "dur",
            "request",
            "longest",
            "pool"
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
                string method = !(axconRequestState == AxCon.RequestState.NotApplicable || axconRequestState == AxCon.RequestState.WaitReq)
                    ? axInfo["lastMethod"].Replace("cmpECommerce", "").Trim('_') : "";
                string longestMethod = axInfo["longestMethod"].Replace("cmpECommerce", "").Trim('_');
                longestMethod = longestMethod.Length <= 14 ? longestMethod : longestMethod.Substring(0, 14);
                longestMethod = (axInfo["longestMethodDuration"] != default(TimeSpan)
                    ? (axInfo["longestMethodDuration"].TotalSeconds < 99
                        ? axInfo["longestMethodDuration"].TotalSeconds.ToString("0")
                        : "99") + "|"
                    : "" ) + longestMethod;
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
                    (axconRequestState == AxCon.RequestState.Request || axconRequestState == AxCon.RequestState.Prepare)
                        && !axcon.GetAsyncRequestTimedOut()
                        && amqpState != AMQP.State.Error
                        && current_req_duration > 0 ? current_req_duration.ToString("0") : "",
                    method.Length <= 14 ? method : method.Substring(0, 14),
                    longestMethod,
                    axInfo["poolCount"]

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
            (exitScheduled ? "Exiting..." : ""),
            isWaitingWorkersExists ? "Init/start/stop scheduled... " : "",
            IsAsyncTaskChainRunning() ? "Async task running..." : ""
        ).Trim());
        output.AppendLine(string.Format(
            "Summary: workers={0} amqpMsg={1} axMsg={2} amqpConnErr={3} axConnErr={4} axRequestError={5} axReqTimedOut={6} msgInQueue<{7}>~{8} pid={9}\n  state={10}",
            workersCount,
            total["amqpMsgCount"],
            total["axMsgCount"],
            total["amqpErrorCount"],
            total["axErrorCount"],
            total["axRequestErrorCount"],
            total["axRequestTimedOutCount"],
            AMQP.queue,
            AMQP.msgInQueue,
            currentProcess.Id,
            state
        ));
        output.AppendLine(string.Format(
            "Config [{0}]:\n workersCheckPeriod={1,2}s !amqpInitTimeout={2}s !axInitTimeout={3}s !axRequestTimeout={4}s startupWorkersCount={5} useClassPool={6}\n methods config timestamp={7}\n logDir=[{8}]",
            Path.GetFullPath(configFile),
            workersCheckPeriod / 1000.0 ,
            amqpInitTimeout / 1000.0,
            axconInitTimeout / 1000.0,
            axconRequestTimeout / 1000.0,
            startupWorkersCount,
            axconUseClassPool ? "yes" : "no",
            AxCon.config["lastModified"].ToString("MM-dd HH:mm:ss"),
            Path.GetFullPath(logDir)
        ));
        PrintToLog(output.ToString() + "\n" + infoMsg, dtNow);
        output.AppendLine(string.Format(
            "Screen update period: {0}s running: {1:d\\.hh\\:mm\\:ss} heap={2:0.0}MB", //private={3:0.0}MB threads={4}
            System.Math.Round(updateScreenPeriod / 1000.0, 1),
            (dtNow - startTime),
            System.Math.Round(Convert.ToSingle(GC.GetTotalMemory(false)) / 1024 / 1024, 1)
            // System.Math.Round(Convert.ToSingle(currentProcess.PrivateMemorySize64) / 1024 / 1024, 1)
            // currentProcess.Threads.Count
        ));
        output.AppendLine("\n?: a: start new worker, d - stop one running worker, <id>d: stop worker by <id>");
        output.AppendLine("?: Ctrl-R: restart, Ctrl-Q, Ctrl-C: stop and exit");
        output.AppendLine("?: <id>p: pause/resume worker by <id>, Ctrl-p: pause/resume all running workers");
        output.AppendLine(string.Format(
            "?: f: force workers check {0,2}, rc: reload methods config",
            nextWorkersCheck != default(DateTime) ? (nextWorkersCheck - dtNow).TotalSeconds.ToString("0"): ""
        ));
        output.AppendLine("Space: pause screen update, Ctrl-C,Ctrl-C - force exit");
        if (infoMsg != "")
        {
            output.AppendLine(string.Format("{0}", "".PadLeft(head.Length - 50, '=')));
            output.AppendLine(infoMsg);
        }
        if (Supervisor.isConsoleAvailable)
        {
            if (Monitor.TryEnter(lockOn, 200))
            {
                try
                {
                    string outputStr = output.ToString();
                    int w = head.Length + 3;
                    int h = outputStr.Split('\n').Length + 3;
                    if (printAutoResetEvent.WaitOne(10))
                    {
                        SetConsoleSize(w, h);
                        Console.Clear();
                        Console.Write("{0}\n{1}", outputStr, userInput);
                    }
                }
                catch (Exception e)
                {
                    dbg.fa(e);
                }
                finally
                {
                    Monitor.Exit(lockOn);
                    printAutoResetEvent.Set();
                }
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
        if (s != lastPrint && dtNow.Second % 2 == 0 && dtNow.Millisecond < 500)
        {
            string fileName = currentProcess.Id + "_" + dtNow.ToString("yyyyMMdd_HHmmss") + ".log";
            string dir = logDir + "\\" + dtNow.ToString("yyyyMMdd") + "\\print";
            try
            {
                Directory.CreateDirectory(dir);
                using (StreamWriter writer = new StreamWriter(dir + "\\" + fileName, false))
                {
                    writer.WriteLine(s);
                }
                lastPrint = s;
            }
            catch {}
        }
    }
    
    public void OnConsoleCancel(object sender, ConsoleCancelEventArgs args)
    {
        if (!exitScheduled) args.Cancel = true;
        UserScheduleApplicationExit();
    }
    
    private void SetConsoleSize(int w, int h)
    {
        if (w <= 0 || w > 200 || h <= 0 || h > 100) return;
        if (w != width || h != height)
        {
            try
            {
                if (w <= Console.LargestWindowWidth)
                    Console.WindowWidth = w;
                if (h <= Console.LargestWindowHeight)
                    Console.WindowHeight = h;
                Console.SetBufferSize(w, h);
                width = w;
                height = h;
            }
            catch (Exception e)
            {
                dbg.fa(string.Format("WxH={0}x{1} {2}", w, h, e.Message));
            }
        }
    }
}
