using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using fastJSON;
using System.Threading;

public class dbg
{
    public static object lock_on = new object();
    
    public static void w(Object obj)
    {
        Console.WriteLine(obj);
    }
    
    public static void ww(Object obj)
    {
        Console.WriteLine("{0} {1} {2}", DateTime.Now.ToString("mm:ss"), Thread.CurrentThread.ManagedThreadId, obj);
    }
    
    public static void wt(Object obj)
    {
        if (obj != null)
            Console.WriteLine(obj.GetType());
        else
            Console.WriteLine("Object is null.");
    }
    
    public static int suf = 1;
    public static void f(Object obj, string prefix = "")
    {
        string dir = "log";
        string ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        prefix = prefix == "" ? prefix : prefix + "_";
        lock (lock_on)
        {
            try
            {
                Directory.CreateDirectory(dir);
                if (File.Exists(dir+"/" + prefix + ts + "_" + Convert.ToString(suf) + ".txt")) suf++; else suf = 1;
                using (StreamWriter writer = File.CreateText(dir+"/" + prefix + ts + "_" + Convert.ToString(suf) + ".txt"))
                {
                    writer.WriteLine(obj.ToString());
                }
            }
            catch {}
        }
    }
    
    public static void fa(Object obj)
    {
        DateTime dtNow = DateTime.Now;
        string dir = AMQPManager.logDir + "\\" + dtNow.ToString("yyyyMMdd");
        string fileName = dir + "\\debug.log";
        lock (lock_on)
        {
            try
            {
                Directory.CreateDirectory(dir);
                if (File.Exists(fileName) && new FileInfo(fileName).Length > 1000000)
                {
                    try {
                        File.Move(fileName, dir + "\\debug" + dtNow.ToString("yyyyMMdd_HHmmss") + ".log");
                    }
                    catch {}
                }
                string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                using (StreamWriter writer = new StreamWriter(fileName, true))
                {
                    writer.WriteLine("{0};{1,2};{2}", ts, Thread.CurrentThread.ManagedThreadId, obj.ToString());
                }
            }
            catch {}
        }
    }
    
    public static string dump(object obj, bool WriteOutput = true)
    {
        string result = "";
        
        JSONParameters jp = new JSONParameters();
        jp.UseEscapedUnicode = false;
        try
        {
            result = JSON.ToNiceJSON(obj, jp);
        }
        catch {}
        if (WriteOutput)
        {
            Console.WriteLine(result);
        }
        return result;
    }
}
