using System;
using System.Collections.Generic;
using System.Threading;
using System.Text;
using System.Text.RegularExpressions;
using System.Diagnostics;

public class AMQPManager
{
    private List<AMQP> worker_list = new List<AMQP>();
    private int worker_count;
    private Thread thrd;
    
    private Process cur_proc = Process.GetCurrentProcess();
    private bool is_receive_input = true;
    private string user_input = "";
    
    private static readonly int worker_max_count = 30;
    private int refresh_period = 1000;
    private int input_poll_period = 300;
    private readonly DateTime start_time = DateTime.Now;
    private readonly bool is_console_available;
    private StringBuilder output = new StringBuilder();
    
    private Dictionary<int,Dictionary<string,int>> stat = new Dictionary<int,Dictionary<string,int>>();
    
    public static void Main(string[] args)
    {
        int count;
        AMQPManager am;
        if (args.Length > 0 && Int32.TryParse(args[0], out count))
        {
            if (count > 0 && count <= worker_max_count)
            {
                am = new AMQPManager(count);
            }
        }
        else
        {
            am = new AMQPManager(1);
        }
    }

    public AMQPManager(int count)
    {
        is_console_available = IsConsoleAvailable();
        thrd = new Thread(Work);
        thrd.Start(count);
        
        // TODO: events or timer
        while (worker_count == 0) Thread.Sleep(100);
        while (worker_count > 0)
        {
            Print();
            // TODO: events or timer
            Thread.Sleep(refresh_period);
        }
        Print();
    }
    
    private void Work(object count)
    {
        AddWorkers((int)count);
        while (is_receive_input)
        {
            if (is_console_available && Console.KeyAvailable) {
                ConsoleKeyInfo ki = Console.ReadKey(true);
                HandleInput(ki);
            }
            // TODO: events or timer
            Thread.Sleep(input_poll_period);
        }
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
        if (GetRunningWorkerCount() < worker_max_count)
        {
            var amqp = new AMQP();
            amqp.Stop += DeleteWorker;
            worker_list.Add(amqp);
            worker_count++;
        }
    }
    
    private void DeleteWorker(AMQP a)
    {
        worker_list.Remove(a);
        worker_count--;
    }

    private void StopWorker(bool stopAll = false)
    {
        foreach (var w in worker_list)
        {
            if (!w.is_stop_pend)
            {
                w.is_stop_pend = true;
                if (!stopAll) break;
            }
        }
    }
    
    private void StopWorkerByNo(int no)
    {
        foreach (var w in worker_list)
        {
            if (w.AMQP_no == no)
            {
                w.is_stop_pend = true;
                break;
            }
        }
    }
    
    private void RestartWorkers()
    {
        int cur_worker_count = GetRunningWorkerCount();
        StopWorker(stopAll: true);
        AddWorkers(cur_worker_count);
    }
    
    private int GetRunningWorkerCount()
    {
        int result = 0;
        foreach (var w in worker_list)
        {
            if (!w.is_stop_pend) result++;
        }
        return result;
    }

    private void HandleInput(ConsoleKeyInfo ki)
    {
        string keyChar = ki.KeyChar.ToString();
        string keyStr = ki.Key.ToString();
        if (user_input == "")
        {
            switch (keyStr)
            {
                case "A":
                case "D":
                    ExecuteCommand(keyStr);
                    break;
                case "R":
                case "Q":
                    if ((ki.Modifiers & ConsoleModifiers.Control) != 0)
                    {
                        ExecuteCommand(keyStr);
                    }
                    break;
                default:
                    if (Regex.IsMatch(keyChar, "[0-9a-zA-Z]"))
                    {
                        user_input += keyChar;
                    }
                    break;
            }
        }
        else
        {
            switch (keyStr)
            {
                case "Enter":
                    ExecuteCommand(user_input);
                    user_input = "";
                    break;
                case "Backspace":
                    user_input = user_input.Remove(user_input.Length-1);
                    break;
                default:
                    if (ki.Key == ConsoleKey.Escape){
                        user_input = "";
                    } else
                    {
                        if (Regex.IsMatch(keyChar, "[0-9a-zA-Z]"))
                        {
                            user_input += keyChar;
                        }
                    }
                    break;
            }
        }
    }
    
    private void ExecuteCommand(string cmd)
    {
        cmd = cmd.ToUpper();
        if (Regex.IsMatch(cmd, @"A|\d*D|R|Q"))
        {
            switch (cmd)
            {
                case "A":
                    AddWorker();
                    is_receive_input = true;
                    break;
                case "D":
                    StopWorker();
                    if (GetRunningWorkerCount() == 0)
                    {
                        is_receive_input = false;
                    }
                    break;
                case "R":
                    RestartWorkers();
                    break;
                case "Q":
                    StopWorker(stopAll: true);
                    is_receive_input = false;
                    break;
                default:
                    if (Regex.IsMatch(cmd, @"\d+D"))
                    {
                        int no = Convert.ToInt32(cmd.Replace("D", ""));
                        StopWorkerByNo(no);
                        if (GetRunningWorkerCount() == 0)
                        {
                            is_receive_input = false;
                        }

                    }
                    break;
            }
        }
    }
    
    private void Print()
    {
        if (!is_console_available)
        {
            return;
        }
        var heap_mem = GC.GetTotalMemory(false);
        output.Clear();
        string tmpl = "{0,3} {1,-20} {2,5} {3,6} {4,5} {5,6} {6,6} {7,9} {8,7:0.0} {9,-20} {10,-20} {11,11} {0,3}";
        string head = string.Format(
            tmpl,
            "no",
            "start_time",
            "a_msg",
            "ax_msg",
            "a_err",
            "ax_err",
            "ax_req",
            "req_start",
            "req_dur",
            "req_class",
            "longest",
            "state"
        );
        output.AppendLine(head);
        output.AppendLine(string.Format("{0}", "".PadLeft(head.Length, '=')));
        for (int i = 0; i < worker_list.Count; i++)
        {
            var amqp = worker_list[i];
            if (!amqp.IsAxReady()) continue;
            int amqp_msg_count = amqp.msg_count;
            int ax_msg_count = amqp.GetAxMsgCount();
            int amqp_err_count = amqp.err_count;
            int ax_err_count = amqp.GetAxErrCount();

            double current_req_duration = System.Math.Round((DateTime.Now - amqp.GetLastReqStartTime()).TotalMilliseconds/1000, 1);
            output.AppendLine(string.Format(
                tmpl,
                amqp.AMQP_no,
                amqp.start_time,
                amqp_msg_count,
                ax_msg_count,
                amqp_err_count,
                ax_err_count,
                amqp.GetIsAxRequesting() ? "yes" : "",
                amqp.GetLastReqStartTime().ToString("HH:mm:ss"),
                amqp.GetIsAxRequesting() && current_req_duration > 0 ? current_req_duration.ToString() : "",
                amqp.GetIsAxRequesting() ? amqp.GetLastMethod().Replace("cmpECommerce", "").Trim('_') : "",
                amqp.GetLongestMethod().Replace("cmpECommerce", "").Trim('_'),
                amqp.is_stop_pend ? "stop_pend" : "running"
            ));
        }
        SaveStat();
        var total = CalcTotalStat();
        output.AppendLine(string.Format(
            "\nSummary: amqp_msg={0} ax_msg={1} amqp_err={2} ax_err={3} workers={4} heap={5}MB private={6}MB msgInQueue={7}",
            total["amqp_msg_count"],
            total["ax_msg_count"],
            total["amqp_err_count"],
            total["ax_err_count"],
            worker_count,
            System.Math.Round(Convert.ToSingle(heap_mem) / 1024 / 1024, 1),
            System.Math.Round(Convert.ToSingle(cur_proc.PrivateMemorySize64) / 1024 / 1024, 1),
            AMQP.msgInQueue.ToString()
        ));
        output.AppendLine(string.Format(
            @"Refresh period: {0}s, running: {1:d\.hh\:mm\:ss}",
            System.Math.Round(refresh_period / 1000.0, 1),
            (DateTime.Now - start_time))
        );
        output.AppendLine("\n?: a: start new worker, [No]d: stop worker [by No], Ctrl-r: restart all workers, Ctrl-q: stop all workers and exit");
        output.AppendLine("Ctrl-C: force exit");
        Console.Clear();
        Console.Write(output.ToString());
        Console.Write(user_input);
    }
    
    private void SaveStat()
    {
        for (int i = 0; i < worker_list.Count; i++)
        {
            var amqp = worker_list[i];
            if (!amqp.IsAxReady()) continue;
            if (!stat.ContainsKey(amqp.AMQP_no))
            {
                stat.Add(
                    amqp.AMQP_no,
                    new Dictionary<string,int>(){
                        {"amqp_msg_count", 0},
                        {"ax_msg_count", 0},
                        {"amqp_err_count", 0},
                        {"ax_err_count", 0}
                    }
                 );
            }
            stat[amqp.AMQP_no]["amqp_msg_count"] = amqp.msg_count;
            stat[amqp.AMQP_no]["ax_msg_count"] = amqp.GetAxMsgCount();
            stat[amqp.AMQP_no]["amqp_err_count"] = amqp.err_count;
            stat[amqp.AMQP_no]["ax_err_count"] = amqp.GetAxErrCount();
        }
    }
    
    private Dictionary<string,int> CalcTotalStat()
    {
        Dictionary<string,int> result = 
            new Dictionary<string,int>(){
                {"amqp_msg_count", 0},
                {"ax_msg_count", 0},
                {"amqp_err_count", 0},
                {"ax_err_count", 0}
            };
        foreach (var i in stat)
        {
            result["amqp_msg_count"] += i.Value["amqp_msg_count"];
            result["ax_msg_count"] += i.Value["ax_msg_count"];
            result["amqp_err_count"] += i.Value["amqp_err_count"];
            result["ax_err_count"] += i.Value["ax_err_count"];
        }
        return result;
    }
    
    private bool IsConsoleAvailable()
    {
        try
        {
            return Console.LargestWindowWidth > 0;
        }
        catch
        {
            return false;
        }
    }
}