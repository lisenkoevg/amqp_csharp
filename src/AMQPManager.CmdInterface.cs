using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;

public partial class AMQPManager
{
    private string userInput = "";
    private string infoMsg = "";
    private string lastPrint = "";
    private int updateScreenPeriod = 500;
    private int inputPollPeriod = 100;
    private int width = 0;
    private int height = 0;
    private bool outputPaused = false;
    private bool inputPaused = false;
    private Timer outputTimer;
    private Timer inputTimer;
    private AutoResetEvent printAutoResetEvent = new AutoResetEvent(true);
    private Dictionary<int,Dictionary<string,int>> workersStatistics = new Dictionary<int,Dictionary<string,int>>();
    
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
        output.AppendLine("Space: pause screen update");
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