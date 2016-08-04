using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using fastJSON;

public class dbg
{
    public static void w(Object obj)
    {
        Console.WriteLine(obj);
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
        Directory.CreateDirectory(dir);
        string ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        prefix = prefix == "" ? prefix : prefix + "_";
        if (File.Exists(dir+"/" + prefix + ts + "_" + Convert.ToString(suf) + ".txt")) suf++; else suf = 1;
        using (StreamWriter writer = File.CreateText(dir+"/" + prefix + ts + "_" + Convert.ToString(suf) + ".txt"))
        {
            writer.WriteLine(obj.ToString());
        }
    }
    
    public static void fa(Object obj, string prefix = "")
    {
        string dir = "log";
        Directory.CreateDirectory(dir);
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        using (StreamWriter writer = new StreamWriter(dir+"/main.log", true))
        {
            writer.WriteLine("{0};{1};{2}", ts, prefix, obj.ToString());
        }
    }
    
    public static string dump(object obj, bool WriteOutput = true)
    {
        string result = "";
        
        JSONParameters jp = new JSONParameters();
        jp.UseEscapedUnicode = false;
        result = JSON.ToNiceJSON(obj, jp);
        if (WriteOutput)
        {
            Console.WriteLine(result);
        }
        return result;
    }
}
