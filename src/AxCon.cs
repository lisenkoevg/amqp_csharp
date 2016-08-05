using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Dynamics.BusinessConnectorNet;
using fastJSON;
using System.Diagnostics;

class AxWarning : Exception
{
    public AxWarning(string msg) : base(msg)
    {}
}

class AxException : Exception
{
    public AxException(string msg) : base(msg)
    {}
}

public class AxCon
{
    private Axapta ax;
    private Dictionary<string,dynamic> config;
    private Dictionary<string,AxaptaObject> ax_class_pool;
    private string client_id;
    private int AMQPNo;
    
    public int err_count = 0;
    public int msg_count = 0;
    public bool is_requesting = false;
    public DateTime last_request_starttime;
    public string last_method;
    
    public TimeSpan longest_method_duration = new TimeSpan();
    public string longest_method = "";
    
    private static string log_dir = "log";
    private object lockOn = new object();
    
    public AxCon(Dictionary<string,dynamic> config, int AMQPNo)
    {
        this.config = config;
        this.AMQPNo = AMQPNo;
        init();
    }
    
    private void init()
    {
        ax_class_pool = new Dictionary<string,AxaptaObject>();
        
        ax = new Axapta();
        Logon();
        
        #if AxMock
        ax = new Axapta(config["methods"]);
        #endif
    }
    
    private void Logon()
    {
        ax.Logon("rba", "ru", "192.168.3.120:2714", "");
    }

    public void Logoff()
    {
        ax.Logoff();
    }

    public Dictionary<string,object> request(string method, Dictionary<string,dynamic> prms, string id = "")
    {
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();
                
        long start_ticks = DateTime.Now.Ticks;
        client_id = null;
        
        Dictionary<string, dynamic> request = new Dictionary<string, object>() {
            {"method", method},
            {"params", prms},
            {"id", id}
        };

        dbg_("request", request);
        
        Dictionary<string, dynamic> response = new Dictionary<string, object>() {
            {"result", null},
            {"error", null},
            {"id", id}
        };

        switch (method)
        {
            case "describe_methods":
                response["result"] = config;
                break;
            case "batch":
                response["result"] = new List<object>();
                Debug.Assert(request["params"] is IList);
                foreach (dynamic sub_param in request["params"])
                {
                    response["result"].Add(this.request(sub_param["method"], sub_param["params"], sub_param["id"]));
                }
                break;
            default:
                try
                {
                    if (!config["methods"].ContainsKey(method))
                    {
                        throw new AxException(string.Format("unknown method: [{0}]", method));
                    }
                    
                    if (!Util.IsNullOrEmptySubitem(prms, "user_hash"))
                    {
                        client_id = Util.md5(prms["user_hash"]);
                    }
                    else
                    {
                        client_id = "";
                    }
                    dynamic method_config = config["methods"][method];
                    AxaptaObject ax_class = this.ax_class(method_config["class"]);
                    set_values(ax_class, method_config["input"], prms);
                    ax_class_call(ax_class, "init");
                    if ((bool)ax_class_call(ax_class, "validate"))
                    {
                        ax_class_call(ax_class, "run");
                        if (method_config.ContainsKey("output"))
                        {
                            response["result"] = get_values(ax_class, method_config["output"]);
                        }
                    }
                    int error_type = (int)ax_class_call(ax_class, "getErrorType");
                    if (error_type != 0)
                    {
                        response["error"] = new Dictionary<string,object>(){
                            {"type", error_type},
                            {"message", ax_class_call(ax_class, "getErrorDescription")}
                        };
                        response["result"] = null;
                    }
                    msg_count++;
                }
                catch (AxWarning e)
                {
                    dbg_("error", 
                        new Dictionary<string,dynamic>(){
                            {"message", e.Message},
                            //todo line number
                            {"line", ""},
                            {"trace", e.ToString()},
                        }, 
                        false
                    );
                    response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                    response["result"] = null;
                    err_count++;
                }
                catch (AxException e)
                {
                    dbg_("error", 
                        new Dictionary<string,string>(){
                            {"message", e.Message},
                            {"line", ""},
                            {"trace", e.ToString()},
                        }, 
                        true
                    );
                    response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                    response["result"] = null;
                    err_count++;
                }
                catch (Exception e)
                {
                    dbg_("fatal", 
                        new Dictionary<string,string>(){
                            {"message", e.Message},
                            {"line", ""},
                            {"trace", e.ToString()},
                        }, 
                        true
                    );
                    response["error"] = new Dictionary<string,string>(){{"message", "fatal error"}};
                    response["result"] = null;
                    err_count++;
                    reload();
                }
                break;
        }
        stopWatch.Stop();
        dbg_(
            "response",
            new Dictionary<string,dynamic>(){
                {"response", response},
                {"elapsed", System.Math.Round(stopWatch.Elapsed.TotalMilliseconds, 0)}
            }
        );
        response["elapsed"] = System.Math.Round(stopWatch.Elapsed.TotalMilliseconds, 0);
        
        return response;
    }    
    
    private object ax_class(string ax_class_name)
    {
        if (!ax_class_pool.ContainsKey(ax_class_name))
        {
            ax_class_pool.Add(ax_class_name, ax.CreateAxaptaObject(ax_class_name));
        }
        return ax_class_pool[ax_class_name];
    }

    private object ax_class_call(AxaptaObject ax_class, string method, params dynamic[] prms)
    {
        last_request_starttime = DateTime.Now;
        last_method = GetKeyByValue(ax_class, ax_class_pool);
        is_requesting = true;
        object result;
        switch (prms.Length)
        {
            case 0:
                result = ax_class.Call(method);
                break;
            case 1:
                result = ax_class.Call(method, prms[0]);
                break;
            case 2:
                result = ax_class.Call(method, prms[0], prms[1]);
                break;
            default:
                throw new AxException("invalid axapta method call");
        }
        
        TimeSpan last_method_duration = DateTime.Now - last_request_starttime;
        if (longest_method == "" || last_method_duration > longest_method_duration)
        {
            longest_method = last_method;
            longest_method_duration = last_method_duration;
        }
        is_requesting = false;
        return result;
    }

    private int value2enum(string val, string enum_type)
    {
        int result;
        if (config["enums"].ContainsKey(enum_type))
        {
            result = config["enums"][enum_type].IndexOf(val);
            if (result == -1)
            {
                throw new AxException(string.Format("value2enum undefined enum value: {0}[{1}]", enum_type, val));
            }
        }
        else
        {
            throw new AxException(string.Format("value2enum unknown type: {0}", enum_type));
        }
        return result;
    }

    private string enum2value(int index, string enum_type)
    {
        string result;
        if (config["enums"].ContainsKey(enum_type))
        {
            try
            {
                result = config["enums"][enum_type][index];
            } catch
            {
                throw new AxException(string.Format("enum2value undefined enum key: {0}[{1}]", enum_type, index));
            }
        }
        else
        {
            throw new AxException(string.Format("enum2value unknown type: {0}", enum_type));
        }
        return result;
    }

    private void set_values(AxaptaObject ax_class, dynamic params_config, dynamic prms)
    {
        for (int i = 0; i < params_config.Count; i++)
        {
            string param_name = params_config[i]["name"];
            dynamic param_config = params_config[i];
            if (Util.IsNullOrEmptySubitem(prms, param_name))
            {
                if (!Util.IsNullOrEmptySubitem(param_config, "mandatory"))
                {
                    throw new AxWarning(string.Format("missing mandatory field: {0}", param_name));
                }
                else
                {
                    continue;
                }
            }
            else
            {
                dynamic val = prms[param_name];
                int iVal = 0;
                DateTime dt = new DateTime();
                string param_type = (!Util.IsNullOrEmptySubitem(param_config, "type"))
                    ? param_config["type"]
                    : "default";
                switch (param_type)
                {
                case "string":
                case "default":
                    ax_class_call(ax_class, param_config["setter"], val);
                    break;
                case "real":
                    float fVal;
                    if (val is string)
                    {
                        val = val.Replace(",", ".");
                        if (Single.TryParse(val, out fVal))
                        {
                            ax_class_call(ax_class, param_config["setter"], fVal);
                        }
                    }
                    break;
                case "integer":
                    if (val is string && Int32.TryParse(val, out iVal))
                    {
                        ax_class_call(ax_class, param_config["setter"], iVal);
                    }
                    break;
                case "boolean":
                    bool bVal;
                    if (val is string)
                    {
                        if (Boolean.TryParse(val, out bVal)) // "true" or "false"
                        {
                            ax_class_call(ax_class, param_config["setter"], bVal);
                        }
                        else if (Int32.TryParse(val, out iVal))
                        {
                            ax_class_call(ax_class, param_config["setter"], iVal != 0);
                        }
                    }
                    break;
                case "date":
                    if (val is string && DateTime.TryParse(val, out dt))
                    {
                        ax_class_call(ax_class, param_config["setter"], dt.ToString("dd.MM.yyyy"));
                    }
                    break;
                case "datetime":
                    if (val is string && DateTime.TryParse(val, out dt))
                    {
                        ax_class_call(ax_class, param_config["setter"], dt.ToString("dd.MM.yyyy HH:mm:ss"));
                    }
                    break;
                case "array":
                    if (!(val is IList))
                    {
                        throw new AxException(string.Format("{0} should be an array", param_name));
                    }
                    for (int j = 0; j < val.Count; j++)
                    {
                        set_values(ax_class, param_config["content"], val[j]);
                        ax_class_call(ax_class, param_config["iterator"]);
                    }
                    break;
                case "hash":
                    if (!(val is IDictionary))
                    {
                        throw new AxException(string.Format("{0} should be an associative array", param_name));
                    }
                    if (param_config.ContainsKey("iterator"))
                    {
                        ax_class_call(ax_class, param_config["iterator"]);
                    }
                    set_values(ax_class, param_config["content"], val);
                    break;
                case "blob":
                        string client_id = "";
                        string file_id = "";
                        if (val is string && val.Length == 65 && val.IndexOf('_') != -1)
                        {
                            var ar = val.Split('_');
                            client_id = ar[0];
                            file_id = ar[1];
                        }
                        else if (val is string && val.Length == 32)
                        {
                            client_id = this.client_id;
                            file_id = val;
                        }
                        else
                        {
                            throw new AxException("invalid data_id");
                        }

                        if (client_id == "")
                        {
                            throw new AxException("empty client_id");
                        }
                        else if (client_id != this.client_id)
                        {
                            throw new AxException("invalid client_id");
                        }
                        if (!Regex.IsMatch(file_id, "^[0-9a-f]{32}$"))
                        {
                            throw new AxException("invalid file_id");
                        }
                        string tmpdir = "c:\\axcon\\axcon\\exchange\\"; // TODO to params2 !!!!!!!!
                        string fname = tmpdir + client_id + '_' + file_id;
                        if (!File.Exists(fname))
                        {
                            throw new AxException("file not found: " + fname);
                        }
                        ax_class_call(ax_class, param_config["setter"], fname);
                    break;
                default:
                    if (val is string && !Util.IsNullOrEmpty(val))
                    {
                        val = value2enum(val, param_config["type"]);
                        ax_class_call(ax_class, param_config["setter"], val);
                    }
                    break;
                }
            }
        }
    }

    private dynamic get_values(AxaptaObject ax_class, dynamic params_config)
    {
        Dictionary<string,dynamic> result = new Dictionary<string,dynamic>();
        dynamic val = null;
        for (int i = 0; i < params_config.Count; i++)
        {
            string param_name = params_config[i]["name"];
            dynamic param_config = params_config[i];

            string param_type = (!Util.IsNullOrEmptySubitem(param_config, "type"))
                ? param_config["type"]
                : "default";
            DateTime dt = new DateTime();
            switch (param_type)
            {
                case "string":
                case "default":
                    val = ax_class_call(ax_class, param_config["getter"]);
                    break;
                case "real":
                    float fVal = 0;
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (val is string && Single.TryParse(val, out fVal))
                    {
                        val = fVal;
                    }
                    break;
                case "integer":
                    int iVal = 0;
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (val is string && Int32.TryParse(val, out iVal))
                    {
                        val = iVal;
                    }
                    break;
                case "boolean":
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (!(val is bool))
                    {
                        var msg = string.Format("Type mismatch in config file: {0} is actually {1}", param_name, val.GetType().Name);
                        // dbg.fa(msg, "TypeMismatch");
                        if (val is int)
                        {
                            val = (val != 0);
                        }
                    }
                    break;
                case "date":
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (val is string && DateTime.TryParse(val, out dt))
                    {
                        val = dt.ToString("yyyy-MM-dd");
                    }
                    else
                    {
                        val = "";
                    }
                    break;
                case "date_time":
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (val is string && DateTime.TryParse(val, out dt))
                    {
                        val = dt.ToString("yyyy-MM-dd HH:mm:ss+04:00");
                    }
                    else
                    {
                        val = "";
                    }
                    break;
                case "array":
                    val = new List<object>();
                    while ((bool)ax_class_call(ax_class, param_config["iterator"]))
                    {
                        val.Add(get_values(ax_class, param_config["content"]));
                    }
                    break;
                case "hash":
                    val = get_values(ax_class, param_config["content"]);
                    break;
                case "blob":
                    // TODO to method
                    // check $this->client_id
                    val = "";
                    if (Util.IsNullOrEmpty(this.client_id))
                    {
                        throw new AxException("empty client_id");
                    }
                    dynamic ax_fname = ax_class_call(ax_class, param_config["getter"]);

                    if (!Util.IsNullOrEmpty(ax_fname) && ax_fname is string)
                    {
                        string fname = Util.ConvertString(ax_fname, FromEncName: "utf-8", ToEncName: "windows-1251");

                        if (Util.IsNullOrEmpty(fname)) {
                            throw new AxException(string.Format("invalid file name [{0}]", ax_fname));
                        }
                        if (!File.Exists(fname))
                        {
                            throw new AxException(string.Format("file not found: {0}",ax_fname));
                        }

                        string file_id = Util.md5(fname);
                        string tmpdir = "c:\\axcon\\axcon\\exchange\\"; // TODO to params
                        string tmpname = tmpdir + this.client_id + '_' + file_id;

                        if (File.Exists(tmpname)) {
                            try { File.Delete(tmpname); } catch {}
                        }
                        try
                        {
                            File.Copy(fname, tmpname);
                        } catch
                        {
                            throw new AxException(string.Format("cannot copy file [{0},{1}]", fname, tmpname));
                        }

                        val = file_id;
                    }
                    break;
                default:
                    val = ax_class_call(ax_class, param_config["getter"]);
                    if (val is int)
                    {
                        val = enum2value(val, param_config["type"]);
                    }
                    break;
            }
            result.Add(param_name, val);
        }
        return result;
    }
    
    private void reload()
    {
        ax_class_pool = new Dictionary<string,AxaptaObject>();
        try
        {
            ax.Logoff();
        } catch
        {
            ax = new Axapta();
        }
        Logon();
    }
    
    private string GetKeyByValue(AxaptaObject val, Dictionary<string,AxaptaObject> dict)
    {
        string result = "";
        foreach (var i in dict)
        {
            if (i.Value == val)
            {
                result = i.Key;
                break;
            }
        }
        return result;
    }
    
    private void dbg_(string type, dynamic a, bool fatal = false)
    {
        JSONParameters prms = new JSONParameters();
        prms.UseEscapedUnicode = false;
        // string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        // Console.WriteLine("{0};{1};{2}", dt, type, JSON.ToNiceJSON(a, prms));
        
        var suf = "";
        if (type != "request" && type != "response")
        {
            suf = "err";
        }
        
        log(string.Format("{0};{1}", type, JSON.ToNiceJSON(a, prms)), suf);
        if (fatal)
        {
            if (!Util.IsNullOrEmpty(config["settings"]["mail_alerts"]))
            {
                string emails = String.Join(",", config["settings"]["mail_alerts"]);
                /*
                @mail(
                    emails
                    'axcon error',
                    JSON.ToNiceJSON(a, prms),
                    "Content-type: text/plain; charset=utf-8\r\n"
                );
                */
            }
        }
    }
    
    // TODO combine with AMQP.log()
    private void log(object obj, string suf = "")
    {
        string basename = "axcon";
        suf = suf != "" ? "_" + suf : "";
        string file_name = basename + AMQPNo + suf;
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        using (StreamWriter writer = new StreamWriter(log_dir + "/" + file_name + ".log", true))
        {
            writer.WriteLine("{0};{1}", ts, obj.ToString());
        }
    }
}