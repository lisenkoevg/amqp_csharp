using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Dynamics.BusinessConnectorNet;
using fastJSON;
using System.Diagnostics;
using System.Threading;

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
    public enum State {Init, Login, Ready, Logoff, InitError, FinError};
    public enum RequestState {NotApplicable, WaitReq, Request, ReqErr};
    private int msgCount = 0;
    private int errorCount = 0;
    private int requestErrorCount = 0;
    public int requestTimedOutCount = 0;
    private State state = State.Init;
    private RequestState requestState = RequestState.NotApplicable;
    private Axapta ax;
    private Dictionary<string,dynamic> config;
    private Dictionary<string,AxaptaObject> axClassPool;
    private string clientId;
    public readonly int workerId;
    private bool asyncInitTimedOut = false;
    private bool asyncRequestTimedOut = false;
    public DateTime lastRequestStarttime;
    public TimeSpan longestMethodDuration = new TimeSpan();
    public string lastMethod = "";
    public string longestMethod = "";

    private object lockOn = new object();

    public AxCon(Dictionary<string,dynamic> config, int workerId)
    {
        this.config = config;
        this.workerId = workerId;
        ax = new Axapta();
        axClassPool = new Dictionary<string,AxaptaObject>();
        #if AxMock
        ax = new Axapta(config["methods"], isThrowExceptions: true);
        #endif
    }

    public void Init()
    {
        InitOrFinAction(Logon, State.Login, State.Ready, RequestState.WaitReq, State.InitError);
    }

    public void Fin()
    {
        InitOrFinAction(Logoff, State.Logoff, State.Init, RequestState.NotApplicable, State.FinError);
    }

    private void InitOrFinAction(Action act, State stateBefore, State stateAfter, RequestState requestStateAfter, State stateIfError)
    {
        // Stopwatch s = Stopwatch.StartNew();
        string msg = "";
        try
        {
            SetState(stateBefore);
            act.Invoke();
            lock (lockOn)
            {
                if (!GetAsyncInitTimedOut())
                {
                    SetState(stateAfter);
                    SetRequestState(requestStateAfter);
                }
                else
                {
                    errorCount++;
                }
            }
        }
        // catch (ServerUnavailableException e)
        // catch (SessionTerminatedException e)
        catch (Exception e)
        {
            log(e, "error");
            msg = e.GetType().Name + " " + e.Message;
            if (!GetAsyncInitTimedOut())
            {
                SetState(stateIfError);
                errorCount++;
            }
        }
        // s.Stop();
        // dbg.fa(string.Format(
            // "{0}.{1} {2} time={3}ms {4} asyncTimedOut={5} {6}",
            // act.Target,
            // act.Method,
            // workerId,
            // s.ElapsedMilliseconds.ToString("0"),
            // GetState(),
            // GetAsyncInitTimedOut(),
            // msg
        // ));
    }

    public bool GetAsyncInitTimedOut()
    {
        lock (lockOn) return asyncInitTimedOut;
    }

    public void SetAsyncInitTimedOut(bool result)
    {
        lock (lockOn) asyncInitTimedOut = result;
    }

    public State GetState()
    {
        return state;
    }

    private void SetState(State state)
    {
        lock(lockOn)
        {
            this.state = state;
        }
    }

    public void SetInitState()
    {
        SetState(State.Init);
    }

    public RequestState GetRequestState()
    {
        lock(lockOn)
            return requestState;
    }

    public void SetRequestState(RequestState val)
    {
        lock(lockOn)
            requestState = val;
    }

    public bool GetAsyncRequestTimedOut()
    {
        lock (lockOn) return asyncRequestTimedOut;
    }

    public void SetAsyncRequestTimedOut(bool result)
    {
        lock (lockOn) asyncRequestTimedOut = result;
    }
    
    private void Logon()
    {
        ax.Logon("rba", "ru", "192.168.3.120:2714", "");
    }

    private void Logoff()
    {
        ax.Logoff();
    }

    public Dictionary<string,object> request(string method, Dictionary<string,dynamic> prms, string id)
    {
        clientId = null;

        Dictionary<string, dynamic> request = new Dictionary<string, object>() {
            {"method", method},
            {"params", prms},
            {"id", id}
        };

        log(request, "request", true);
        log(request, "", true, true);

        Dictionary<string, dynamic> response = new Dictionary<string, object>() {
            {"result", null},
            {"error", null},
            {"id", id},
            {"elapsed", 0}
        };
        bool aReqTimedOut = false;
        string fileSuffix;
        switch (method)
        {
            case "describe_methods":
                response["result"] = new Dictionary<string,object>() {
                    {"enums", config["enums"]},
                    {"methods", config["methods"]}
                };
                break;
            default:
                SetRequestState(RequestState.Request);
                try
                {
                    if (!config["methods"].ContainsKey(method))
                    {
                        throw new AxException(string.Format("unknown method: [{0}]", method));
                    }

                    if (!Util.IsNullOrEmptySubitem(prms, "user_hash"))
                    {
                        clientId = Util.md5(prms["user_hash"]);
                    }
                    else
                    {
                        clientId = "";
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
                    aReqTimedOut = GetAsyncRequestTimedOut();
                    if (!aReqTimedOut)
                    {
                        SetRequestState(RequestState.WaitReq);
                    }
                }
                catch (AxWarning e)
                {
                    aReqTimedOut = GetAsyncRequestTimedOut();
                    fileSuffix = !aReqTimedOut ? "error" : "error_timedout_skipped";
                    log(e, fileSuffix, true);
                    if (!aReqTimedOut)
                    {
                        SetRequestState(RequestState.WaitReq);
                        response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                        response["result"] = null;
                    }
                }
                catch (AxException e)
                {
                    aReqTimedOut = GetAsyncRequestTimedOut();
                    fileSuffix = !aReqTimedOut ? "request_error" : "error_timedout_skipped";
                    log(e, fileSuffix, true);
                    if (!aReqTimedOut)
                    {
                        SetRequestState(RequestState.WaitReq);
                        response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                        response["result"] = null;
                    }
                }
                catch (Exception e)
                {
                    aReqTimedOut = GetAsyncRequestTimedOut();
                    fileSuffix = !aReqTimedOut ? "request_fatal" : "request_fatal_timedout_skipped";
                    log(e, fileSuffix, false);
                    if (!aReqTimedOut)
                    {
                        SetRequestState(RequestState.ReqErr);
                        requestErrorCount++;
                        response["error"] = new Dictionary<string,string>(){{"message", "fatal error"}};
                        response["result"] = null;
                    }
                }
                break;
        }
        msgCount++;
        return response;
    }

    public Dictionary<string,dynamic> GetInfo()
    {
        return new Dictionary<string,object>() {
            {"msgCount", msgCount},
            {"errorCount", errorCount},
            {"requestErrorCount", requestErrorCount},
            {"requestTimedOutCount", requestTimedOutCount},
            {"lastRequestStarttime", lastRequestStarttime},
            {"lastMethod", lastMethod},
            {"longestMethod", longestMethod},
            {"longestMethodDuration", longestMethodDuration}
        };
    }

    private object ax_class(string ax_class_name)
    {
        if (!axClassPool.ContainsKey(ax_class_name))
        {
            axClassPool.Add(ax_class_name, ax.CreateAxaptaObject(ax_class_name));
        }
        return axClassPool[ax_class_name];
    }

    private object ax_class_call(AxaptaObject ax_class, string method, params dynamic[] prms)
    {
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
        foreach (var item in params_config)
        {
            string param_name = item.Key;
            dynamic param_config = item.Value;
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
                        string clientId = "";
                        string file_id = "";
                        if (val is string && val.Length == 65 && val.IndexOf('_') != -1)
                        {
                            var ar = val.Split('_');
                            clientId = ar[0];
                            file_id = ar[1];
                        }
                        else if (val is string && val.Length == 32)
                        {
                            clientId = this.clientId;
                            file_id = val;
                        }
                        else
                        {
                            throw new AxException("invalid data_id");
                        }

                        if (clientId == "")
                        {
                            throw new AxException("empty clientId");
                        }
                        else if (clientId != this.clientId)
                        {
                            throw new AxException("invalid clientId");
                        }
                        if (!Regex.IsMatch(file_id, "^[0-9a-f]{32}$"))
                        {
                            throw new AxException("invalid file_id");
                        }
                        string tmpdir = "c:\\axcon\\axcon\\exchange\\"; // TODO to params2 !!!!!!!!
                        string fname = tmpdir + clientId + '_' + file_id;
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
        foreach (var item in params_config)
        {
            string param_name = item.Key;
            dynamic param_config = item.Value;

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
                    // check $this->clientId
                    val = "";
                    if (Util.IsNullOrEmpty(this.clientId))
                    {
                        throw new AxException("empty clientId");
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
                        string tmpname = tmpdir + this.clientId + '_' + file_id;

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

    private static object lockOnSt = new object();
    public void log(object obj, string fileSuffix = "", bool toJSON = false, bool includeWorkerIdToFileName = false)
    {
        fileSuffix = (fileSuffix != "") ? "_" + fileSuffix : "";
        DateTime dtNow = DateTime.Now;
        string timestamp = dtNow.ToString("yyyy-MM-dd HH:mm:ss");
        string basename = this.GetType().Name;
        string file_name = basename + (includeWorkerIdToFileName ? workerId.ToString() : "") + fileSuffix + ".log";
        string dir = AMQPManager.logDir + "\\" + dtNow.ToString("yyyyMMdd") + "\\" + (includeWorkerIdToFileName ? "byWorkerId" : "");
        //TODO move directory creation to another place
        Directory.CreateDirectory(dir);
        
        if (!includeWorkerIdToFileName)
        {
            Monitor.Enter(lockOnSt);
        }
        try
        {
            using (StreamWriter writer = new StreamWriter(dir + "\\" + file_name, true))
            {
                if (!toJSON)
                {
                    writer.WriteLine("{0};{1,3};{2}", timestamp, workerId, obj.ToString());
                }
                else
                {
                    JSONParameters prms = new JSONParameters();
                    prms.UseEscapedUnicode = false;
                    writer.WriteLine("{0};{1,3};{2}", timestamp, workerId, JSON.ToJSON(obj, prms));
                }
            }
        }
        finally
        {
            if (!includeWorkerIdToFileName)
            {
                Monitor.Exit(lockOnSt);
            }
        }
        /*
        if (fatal)
        {
            if (!Util.IsNullOrEmpty(config["settings"]["mail_alerts"]))
            {
                string emails = String.Join(",", config["settings"]["mail_alerts"]);
                @mail(
                    emails
                    'axcon error',
                    JSON.ToNiceJSON(a, prms),
                    "Content-type: text/plain; charset=utf-8\r\n"
                );
            }
        }
        */
    }
}
