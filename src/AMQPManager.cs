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
    private bool is_receive_commands = true;
    private bool output_paused = false;
    private string user_input = "";
    
    private static readonly int worker_max_count = 30;
    private readonly int fatal_err_threshold = 5;
    private int refresh_period = 500;
    private int input_poll_period = 300;
    private readonly DateTime start_time = DateTime.Now;
    private readonly bool is_console_available;
    private StringBuilder output = new StringBuilder();
    private Dictionary<int,Dictionary<string,int>> stat = new Dictionary<int,Dictionary<string,int>>();
    private int fatal_err_count;
    
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
        
        do
        {
            if (!output_paused) Print();
            // TODO: add Timer
            Thread.Sleep(refresh_period);
        }
        while (worker_count > 0);
        Print();
    }
    
    private void Work(object count)
    {
        AddWorkers((int)count);
        while (is_receive_commands)
        {
            if (is_console_available && Console.KeyAvailable) {
                ConsoleKeyInfo ki = Console.ReadKey(true);
                HandleInput(ki);
            }
            // TODO: add Timer
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
            amqp.OnStop += DeleteWorker;
            amqp.OnFatalError += FatalErrorHandler;
            worker_list.Add(amqp);
            worker_count++;
        }
    }
    
    private void DeleteWorker(AMQP a)
    {
        worker_list.Remove(a);
        worker_count--;
    }
    
    private void FatalErrorHandler(AMQP a)
    {
        fatal_err_count++;
        if (fatal_err_count < fatal_err_threshold)
        {
            AddWorker();
        }
        else
        {
            is_receive_commands = false;
        }
    }
    
    private void StopWorker()
    {
        foreach (var w in worker_list)
        {
            if (!w.is_stop_pend)
            {
                w.is_stop_pend = true;
                break;
            }
        }
    }
    
    private void StopAllWorkers()
    {
        foreach (var w in worker_list)
        {
            w.is_stop_pend = true;
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
        StopAllWorkers();
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
        if (output_paused && keyStr != "Spacebar")
        {
            return;
        }
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
                case "Spacebar":
                    ToggleOutputPaused();
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
                case "Spacebar":
                    ToggleOutputPaused();
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
                    break;
                case "D":
                    StopWorker();
                    if (GetRunningWorkerCount() == 0)
                    {
                        is_receive_commands = false;
                    }
                    break;
                case "R":
                    RestartWorkers();
                    break;
                case "Q":
                    StopAllWorkers();
                    is_receive_commands = false;
                    break;
                default:
                    if (Regex.IsMatch(cmd, @"\d+D"))
                    {
                        int no = Convert.ToInt32(cmd.Replace("D", ""));
                        StopWorkerByNo(no);
                        if (GetRunningWorkerCount() == 0)
                        {
                            is_receive_commands = false;
                        }
                    }
                    break;
            }
        }
    }
    
    private void ToggleOutputPaused()
    {
        string cap = "(paused) ";
        output_paused = !output_paused;
        if (output_paused)
            Console.Title = cap + Console.Title;
        else
            Console.Title = Console.Title.Replace(cap, "");
    }
    
    private void Print()
    {
        if (!is_console_available)
        {
            return;
        }
        var heap_mem = GC.GetTotalMemory(false);
        output.Clear();
        string tmpl = "{0,3} {1,-19} {13,-9} {6,-6} {2,5} {4,5} {3,6} {5,6} {7,-5} {8,-6} {9,9} {10,7} {11,-20} {12,-20} {0,3}";
        
        string head = string.Format(
            tmpl,
            "no",
            "start_time",
            "a_msg",
            "ax_msg",
            "a_err",
            "ax_err",
            "a_proc",
            "ax_lo",
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
            var ax_info = amqp.GetAxInfo();
            double current_req_duration = System.Math.Round((DateTime.Now - ax_info["last_request_starttime"]).TotalMilliseconds/1000, 1);
            output.AppendLine(string.Format(
                tmpl,
                amqp.AMQP_no,
                amqp.start_time,
                amqp.msg_count,
                ax_info["msg_count"],
                amqp.err_count,
                ax_info["err_count"],
                amqp.IsProcessing() ? "yes" : "",
                ax_info["is_logged_on"] ? "yes" : "",
                ax_info["is_requesting"] ? "yes" : "",
                ax_info["last_request_starttime"] != default(DateTime) ? ax_info["last_request_starttime"].ToString("HH:mm:ss") : "",
                ax_info["is_requesting"] && current_req_duration > 0 ? current_req_duration.ToString("0.0") : "",
                ax_info["is_requesting"] ? ax_info["last_method"].Replace("cmpECommerce", "").Trim('_') : "",
                ax_info["longest_method"].Replace("cmpECommerce", "").Trim('_'),
                amqp.is_stop_pend ? "stop_pend" : "running"
            ));
        }
        SaveStat();
        var total = CalcTotalStat();
        output.AppendLine(string.Format(
            "\nSummary: amqp_msg={0} ax_msg={1} amqp_err={2} ax_err={3} workers={4} heap={5:0.0}MB private={6:0.0}MB msgInQueue={7} fatal_err={8}",
            total["amqp_msg_count"],
            total["ax_msg_count"],
            total["amqp_err_count"],
            total["ax_err_count"],
            worker_count,
            System.Math.Round(Convert.ToSingle(heap_mem) / 1024 / 1024, 1),
            System.Math.Round(Convert.ToSingle(cur_proc.PrivateMemorySize64) / 1024 / 1024, 1),
            AMQP.msgInQueue.ToString(),
            fatal_err_count
        ));
        output.AppendLine(string.Format(
            @"Refresh period: {0}s, running: {1:d\.hh\:mm\:ss}",
            System.Math.Round(refresh_period / 1000.0, 1),
            (DateTime.Now - start_time))
        );
        output.AppendLine("\n?: a: start new worker, [No]d: stop worker [by No], Ctrl-r: restart all workers, Ctrl-q: stop all workers and exit");
        output.AppendLine("Space: pause, Ctrl-C: force exit");
        Console.Clear();
        Console.Write(output.ToString());
        Console.Write(user_input);
    }
    
    private void SaveStat()
    {
        for (int i = 0; i < worker_list.Count; i++)
        {
            var amqp = worker_list[i];
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
            var ax_info = amqp.GetAxInfo();
            stat[amqp.AMQP_no]["amqp_msg_count"] = amqp.msg_count;
            stat[amqp.AMQP_no]["ax_msg_count"] = ax_info["msg_count"];
            stat[amqp.AMQP_no]["amqp_err_count"] = amqp.err_count;
            stat[amqp.AMQP_no]["ax_err_count"] = ax_info["err_count"];
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