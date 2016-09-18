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

    private readonly DateTime startTime = DateTime.Now;
    private readonly Process currentProcess = Process.GetCurrentProcess();
    private Process parentProcess;

    private Dictionary<int, AMQP> amqpDic = new Dictionary<int, AMQP>();
    private Dictionary<int, AxCon> axconDic = new Dictionary<int, AxCon>();
    private Timer checkWorkersTimer;
    private AutoResetEvent workAutoResetEvent = new AutoResetEvent(false);
    private AutoResetEvent checkWorkersAutoResetEvent = new AutoResetEvent(true);
    private int _workerId = 0;
    private int workersCount = 0;
    private bool exitScheduled = false;
    private bool isBusinessConnectorInstanceInvalid;
    private DateTime nextWorkersCheck = default(DateTime);
    private Task asyncTaskChainHead = new Task(()=>{});
    private Task asyncTaskChainTail = null;
    private object lockOn = new object();
    private Logger logger = new Logger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);

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
        psi.Arguments = string.Format("/c {0}{1} -parentPID:{2}", exeName, config, parentProcessId);
        if (redirectError)
        {
            psi.Arguments += string.Format(" 2>>\"{0}\\{1}{2}.{3}.log\"", logDir, prefix, parentProcessId, id);
        }
        result = Process.Start(psi);
        result = Supervisor.WaitForChildProcess(result.Id, Path.GetFileName(exeName));
        ClearEmptyExceptionLogs(prefix);
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
            infoMsg += descr;
        }
    }
}
