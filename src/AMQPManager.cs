using System;
using System.Collections.Generic;
using System.Threading;
using System.Text;

public class AMQPManager
{
    private List<AMQP> lst = new List<AMQP>();
    private Thread thrd;
    private int thrdCount;
    
    private int refreshDisplayPeriod = 500;
    
    public static void Main(string[] args)
    {
        int count;
        AMQPManager am;
        if (args.Length > 0 && Int32.TryParse(args[0], out count))
        {
            am = new AMQPManager(count);
        }
        else
        {
            am = new AMQPManager(1);
        }
    }

    public AMQPManager(int count)
    {
        thrdCount = count;
        thrd = new Thread(Work);
        thrd.Start();
        
        while (true)
        {
            Print();
            Thread.Sleep(refreshDisplayPeriod);
        }
    }
    
    private void Work()
    {
        for (int i = 0; i < thrdCount; i++)
        {
            var amqp = new AMQP();
            lst.Add(amqp);
            Thread.Sleep(100);
        }

        // for (int i = 0; i < lst.Count; i++)
        // {
            // lst[i].isRunning = false;
        // }
        
        // for (int i = 0; i < lst.Count; i++)
        // {
            // lst[i].thrd.Join();
        // }
        // lst.Clear();
    }
    
    private void Print()
    {
        var sb = new StringBuilder();
        if (lst.Count == 0)
        {
            return;
        }
        Console.Clear();
        string tmpl = "{0,3} {1,-20} {2,8} {3,6} {4,8} {5,6} {6,9} {7,9} {8,8} {9,-20} {10,-20}";
        string head = string.Format(
            tmpl,
            "No",
            "StartTime",
            "amqp_msg",
            "ax_msg",
            "amqp_err",
            "ax_err",
            "is_ax_req",
            "req_start",
            "req_dur",
            "req_class",
            "longest"
        );
        sb.AppendLine(head);
        sb.AppendLine(string.Format("{0}", "".PadLeft(head.Length, '=')));
        var total = new Dictionary<string,int>(){
            {"amqp_msg_count", 0},
            {"ax_msg_count", 0},
            {"amqp_err_count", 0},
            {"ax_err_count", 0}
        };
        for (int i = 0; i < lst.Count; i++)
        {
            var amqp = lst[i];
            if (!amqp.IsAxReady()) continue;
            int amqp_msg_count = amqp.msg_count;
            int ax_msg_count = amqp.GetAxMsgCount();
            int amqp_err_count = amqp.err_count;
            int ax_err_count = amqp.GetAxErrCount();
            
            total["amqp_msg_count"] += amqp_msg_count;
            total["ax_msg_count"] += ax_msg_count;
            total["amqp_err_count"] += amqp_err_count;
            total["ax_err_count"] += ax_err_count;
            
            double current_req_duration = System.Math.Round((DateTime.Now - amqp.GetLastReqStartTime()).TotalMilliseconds/1000, 1);
            sb.AppendLine(string.Format(
                tmpl,
                amqp.AMQPNo,
                amqp.startTime,
                amqp_msg_count,
                ax_msg_count,
                amqp_err_count,
                ax_err_count,
                amqp.GetIsAxRequesting() ? "yes" : "no",
                amqp.GetLastReqStartTime().ToString("HH:mm:ss"),
                amqp.GetIsAxRequesting() && current_req_duration > 0 ? current_req_duration.ToString() : "",
                amqp.GetIsAxRequesting() ? amqp.GetLastMethod().Replace("cmpECommerce", "").Trim('_') : "",
                amqp.GetLongestMethod().Replace("cmpECommerce", "").Trim('_')
            ));
        }
        sb.AppendLine(string.Format(
            "\nTotal: amqp_msg={0} ax_msg={1} amqp_err={2} ax_err={3} ",
            total["amqp_msg_count"],
            total["ax_msg_count"],
            total["amqp_err_count"],
            total["ax_err_count"]
        ));
        sb.AppendLine(string.Format("Refresh period: {0}ms", refreshDisplayPeriod));
        sb.AppendLine("\n(Press Ctrl-C to exit)");
        Console.Write(sb.ToString());
    }
}