using System;
using System.Threading;
using System.IO;
using System.Diagnostics;
using fastJSON;

public class Logger
{
    private static string baseDir = AMQPManager.logDir;
    private string baseName;
    private object lockOn = new object();
    private static int pid = Process.GetCurrentProcess().Id;

    public Logger(string baseName)
    {
        this.baseName = baseName;
    }

    public void Log(int workerId, object obj, string fileSuffix = "", bool includeWorkerIdToFileName = false)
    {
        Log_(workerId, obj, fileSuffix, includeWorkerIdToFileName, false);
    }
    
    public void LogInJSON(int workerId, object obj, string fileSuffix = "", bool includeWorkerIdToFileName = false)
    {
        Log_(workerId, obj, fileSuffix, includeWorkerIdToFileName, true);
    }
    
    private void Log_(int workerId, object obj, string fileSuffix, bool includeWorkerIdToFileName, bool inJSON)
    {
        int tries = 100;
        int i = 0;
        while(!TryLog(workerId, obj, fileSuffix, includeWorkerIdToFileName, inJSON) && i < tries)
        {
            Thread.Sleep(10);
            i++;
        }
        if (i > 0)
        {
            dbg.fa(string.Format("{0} log iteration is {1}, fileSuffix={2}", baseName, i, fileSuffix));
        }
    }
    
    private bool TryLog(int workerId, object obj, string fileSuffix, bool includeWorkerIdToFileName, bool inJSON)
    {
        bool success = false;
        fileSuffix = (fileSuffix != "") ? "_" + fileSuffix : "";
        DateTime dtNow = DateTime.Now;
        string timestamp = dtNow.ToString("yyyy-MM-dd HH:mm:ss");
        string file_name = baseName + (includeWorkerIdToFileName ? workerId.ToString() : "") + fileSuffix + ".log";
        string dir = baseDir + "\\" + dtNow.ToString("yyyyMMdd") + "\\" + (includeWorkerIdToFileName ? "byWorkerId" : "");
        dynamic dynObj = obj;
        Directory.CreateDirectory(dir);

        if (!includeWorkerIdToFileName)
        {
            Monitor.Enter(lockOn);
        }
        try
        {
            string msg = "";
            if (!inJSON)
            {
                msg = obj.ToString();
            }
            else
            {
                JSONParameters prms = new JSONParameters();
                prms.UseEscapedUnicode = false;
                try
                {
                    msg = JSON.ToJSON(obj, prms);
                    msg = Util.CutUserHash(msg);
                }
                catch (Exception e)
                {
                    msg = e.ToString();
                    try { msg += "\nerror['message']=" + dynObj["error"]["message"]; } catch {}
                }
            }
            using (StreamWriter writer = new StreamWriter(dir + "\\" + file_name, true))
            {
                writer.WriteLine("{0};pid={1};wid={2,2};{3}", timestamp, pid, workerId, msg);
            }
            success = true;
        }
        finally
        {
            if (!includeWorkerIdToFileName)
            {
                Monitor.Exit(lockOn);
            }
        }
        return success;
    }
}