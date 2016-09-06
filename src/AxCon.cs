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
    public enum RequestState {NotApplicable, WaitReq, Prepare, PrepOk, PrepWarn, PrepErr, Request, ReqErr};
    private int msgCount = 0;
    private int errorCount = 0;
    private int requestErrorCount = 0;
    public int requestTimedOutCount = 0;
    private State state = State.Init;
    private RequestState requestState = RequestState.NotApplicable;
    private Axapta ax;
    private Dictionary<string,AxaptaObject> axClassPool = new Dictionary<string,AxaptaObject>();
    private string clientId;
    public readonly int workerId;
    private bool asyncInitTimedOut = false;
    private bool asyncRequestTimedOut = false;
    public DateTime lastRequestStarttime;
    public TimeSpan longestMethodDuration = new TimeSpan();
    public string lastMethod = "";
    public string longestMethod = "";
    public bool useClassPool = true;
    public static Dictionary<string,dynamic> config;
    private Dictionary<string, object> request;
    private Dictionary<string, object> response;
    private AxaptaObject axClass;
    private dynamic methodConfig;
    private static readonly Dictionary<string,object> errorMsg =
        new Dictionary<string,object>(){{"code", -32000}, {"message", "Server error"}};
    private object lockOn = new object();
    private static int pid = Process.GetCurrentProcess().Id;
    public Stopwatch stopwatch = new Stopwatch();
    public bool isBusinessConnectorInstanceInvalid = false;
    public event Action OnProcessCorruptedStateException;
    public static Logger logger = new Logger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);

    public AxCon(int workerId)
    {
        this.workerId = workerId;
        ax = new Axapta();
    }

    static AxCon()
    {
        LoadConfig();
    }

    public static string LoadConfig()
    {
        string result = "";
        try
        {
            var tmp = ConfigLoader.Load("./config");
            config = tmp;
        }
        catch (Exception e)
        {
            result = e.Message;
        }
        finally
        {
            if (config == null)
            {
                Console.WriteLine(result);
                Environment.Exit(1);
            }
        }
        return result;
    }

    [System.Runtime.ExceptionServices.HandleProcessCorruptedStateExceptions]
    [System.Security.SecurityCritical]
    public void Init()
    {
        Stopwatch s = Stopwatch.StartNew();
        string msg = "";
        try
        {
            SetState(State.Login);
            Logon();
            lock (lockOn)
            {
                if (!GetAsyncInitTimedOut())
                {
                    SetState(State.Ready);
                    SetRequestState(RequestState.WaitReq);
                }
                else
                {
                    errorCount++;
                }
            }
        }
        // catch (ServerUnavailableException e)
        // catch (SessionTerminatedException e)
        // catch (AlreadyLoggedOnException e)
        catch (AccessViolationException e)
        {
            OnProcessCorruptedStateException();
            SetState(State.InitError);
            errorCount++;
            logger.Log(workerId, string.Format(
                "{0} Init()",
                e.GetType()
            ), "AccessViolationException");
            msg = e.GetType().Name + " " + e.Message;
        }
        catch (Exception e)
        {
            if (e is BusinessConnectorInstanceInvalid)
            {
                isBusinessConnectorInstanceInvalid = true;
            }
            if (!GetAsyncInitTimedOut())
            {
                SetState(State.InitError);
                errorCount++;
            }
            msg = e.GetType().Name + " " + e.Message;
        }
        s.Stop();
        logger.Log(workerId, string.Format(
            "time={0}ms {1} asyncTimedOut={2} {3}",
            s.ElapsedMilliseconds.ToString("0"),
            GetState(),
            GetAsyncInitTimedOut(),
            msg
        ), "init");
    }

    [System.Runtime.ExceptionServices.HandleProcessCorruptedStateExceptions]
    [System.Security.SecurityCritical]
    public void Fin()
    {
        Stopwatch s = Stopwatch.StartNew();
        string msg = "";
        try
        {
            SetState(State.Logoff);
            Logoff();
            lock (lockOn)
            {
                if (!GetAsyncInitTimedOut())
                {
                    SetState(State.Init);
                    SetRequestState(RequestState.NotApplicable);
                }
                else
                {
                    errorCount++;
                }
            }
        }
        // catch (ServerUnavailableException e)
        // catch (SessionTerminatedException e)
        // catch (BusinessConnectorInstanceInvalid e)
        catch (AccessViolationException e)
        {
            OnProcessCorruptedStateException();
            SetState(State.FinError);
            errorCount++;
            logger.Log(workerId, string.Format(
                "{0} Fin()",
                e.GetType()
            ), "AccessViolationException");
            msg = e.GetType().Name + " " + e.Message;
        }
        catch (Exception e)
        {
            if (!GetAsyncInitTimedOut())
            {
                SetState(State.FinError);
                errorCount++;
            }
            msg = e.GetType().Name + " " + e.Message;
        }
        axClassPool.Clear();
        ax = new Axapta();
        s.Stop();
        logger.Log(workerId, string.Format(
            "time={0}ms {1} asyncTimedOut={2} {3}",
            s.ElapsedMilliseconds.ToString("0"),
            GetState(),
            GetAsyncInitTimedOut(),
            msg
        ), "fin");
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

    public RequestState PrepareRequest(string method, Dictionary<string,dynamic> prms, string id)
    {
        SetRequestState(RequestState.Prepare);
        bool aReqTimedOut = false;
        string logFileSuffix;
        clientId = null;
        request = new Dictionary<string, object>() {
            {"method", method},
            {"params", prms},
            {"id", id}
        };
        logger.LogInJSON(workerId, request, "request");
        logger.LogInJSON(workerId, request, "", true);
        response = new Dictionary<string, object>() {
            {"result", null},
            {"error", null},
            {"id", id},
            {"elapsed", 0}
        };
        if (method == "describe_methods")
        {
            SetRequestState(RequestState.PrepOk);
            return GetRequestState();
        }
        try
        {
            if (!config["methods"].ContainsKey(method))
            {
                throw new AxException(string.Format("unknown method: [{0}]", method));
            }
            clientId = !Util.IsNullOrEmptySubitem(prms, "user_hash") ? Util.md5(prms["user_hash"]) : "";
            methodConfig = config["methods"][method];
            axClass = GetAxClass(methodConfig["class"]);
            ax_class_call(axClass, "clear");
            set_values(axClass, methodConfig["input"], prms);
            ax_class_call(axClass, "init");
            bool validateSuccess = (bool)ax_class_call(axClass, "validate");
            aReqTimedOut = GetAsyncRequestTimedOut();
            if (!aReqTimedOut)
            {
                if (validateSuccess)
                {
                    SetRequestState(RequestState.PrepOk);
                }
                else
                {
                    int error_type = (int)ax_class_call(axClass, "getErrorType");
                    if (error_type != 0)
                    {
                        response["error"] = new Dictionary<string,object>(){
                            {"type", error_type},
                            {"message", ax_class_call(axClass, "getErrorDescription")}
                        };
                        response["result"] = null;
                    }
                    SetRequestState(RequestState.PrepWarn);
                }
            }
        }
        catch (AxWarning e)
        {
            aReqTimedOut = GetAsyncRequestTimedOut();
            logFileSuffix = !aReqTimedOut ? "prepare_axWarning" : "prepare_axWarning_timedOut_skipped";
            LogException(e, logFileSuffix);
            if (!aReqTimedOut)
            {
                SetRequestState(RequestState.PrepWarn);
                response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                response["result"] = null;
            }
        }
        catch (AxException e)
        {
            aReqTimedOut = GetAsyncRequestTimedOut();
            logFileSuffix = !aReqTimedOut ? "prepare_axException" : "prepare_axException_timedOut_skipped";
            LogException(e, logFileSuffix);
            if (!aReqTimedOut)
            {
                SetRequestState(RequestState.PrepWarn);
                response["error"] = new Dictionary<string,string>(){{"message", e.Message}};
                response["result"] = null;
            }
        }
        catch (Exception e)
        {
            if (e is BusinessConnectorInstanceInvalid)
            {
                isBusinessConnectorInstanceInvalid = true;
            }
            aReqTimedOut = GetAsyncRequestTimedOut();
            logFileSuffix = !aReqTimedOut ? "prepare_exception" : "prepare_exception_timedOut_skipped";
            LogException(e, logFileSuffix);
            if (!aReqTimedOut)
            {
                SetRequestState(RequestState.PrepErr);
                requestErrorCount++;
                response["error"] = errorMsg;
                response["result"] = null;
            }
        }
        return GetRequestState();
    }

    public Dictionary<string,object> ExecuteRequest()
    {
        if (GetRequestState() == RequestState.PrepWarn)
        {
            SetRequestState(RequestState.WaitReq);
            msgCount++;
            return response;
        }
        SetRequestState(RequestState.Request);
        if ((string)request["method"] == "describe_methods")
        {
            response["result"] = new Dictionary<string,object>() {
                {"enums", config["enums"]},
                {"methods", config["methods"]}
            };
            SetRequestState(RequestState.WaitReq);
            msgCount++;
            return response;
        }
        bool aReqTimedOut = false;
        string logFileSuffix;
        try
        {
            ax_class_call(axClass, "run");
            if (methodConfig.ContainsKey("output"))
            {
                response["result"] = get_values(axClass, methodConfig["output"]);
            }
            int error_type = (int)ax_class_call(axClass, "getErrorType");
            if (error_type != 0)
            {
                response["error"] = new Dictionary<string,object>(){
                    {"type", error_type},
                    {"message", ax_class_call(axClass, "getErrorDescription")}
                };
                response["result"] = null;
            }
            aReqTimedOut = GetAsyncRequestTimedOut();
            if (!aReqTimedOut)
            {
                SetRequestState(RequestState.WaitReq);
            }
        }
        catch (AxException e)
        {
            aReqTimedOut = GetAsyncRequestTimedOut();
            logFileSuffix = !aReqTimedOut ? "request_axException" : "request_axException_timedOut_skipped";
            LogException(e, logFileSuffix);
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
            logFileSuffix = !aReqTimedOut ? "request_exception" : "request_exception_timedOut_skipped";
            LogException(e, logFileSuffix);
            if (!aReqTimedOut)
            {
                SetRequestState(RequestState.ReqErr);
                requestErrorCount++;
                response["error"] = errorMsg;
                response["result"] = null;
            }
        }
        msgCount++;
        return response;
    }

    private void LogException(Exception e, string logFileSuffix)
    {
        logger.Log(
            workerId,
            string.Format(
                "method={0};params={1};id={2};{3}",
                request["method"],
                Util.CutUserHash(Util.ToJSON( request["params"])),
                request["id"],
                e.ToString()
            ),
            logFileSuffix
        );
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
            {"longestMethodDuration", longestMethodDuration},
            {"poolCount", axClassPool.Count}
        };
    }

    private AxaptaObject GetAxClass(string ax_class_name)
    {
        if (!axClassPool.ContainsKey(ax_class_name) || !useClassPool)
        {
            axClassPool[ax_class_name] = ax.CreateAxaptaObject(ax_class_name);
        }
        return axClassPool[ax_class_name];
    }

    [System.Runtime.ExceptionServices.HandleProcessCorruptedStateExceptions]
    [System.Security.SecurityCritical]
    private object ax_class_call(AxaptaObject ax_class, string method, params dynamic[] prms)
    {
        object result = null;
        try
        {
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
        }
        catch (AccessViolationException e)
        {
            OnProcessCorruptedStateException();
            logger.Log(workerId, string.Format(
                "{0} ax_class_call() class={1} method={2} params={3}",
                e.GetType(),
                GetKeyByValue(ax_class, axClassPool),
                method,
                Util.CutUserHash(Util.ToJSON(prms))
            ), "AccessViolationException");
            throw e;
        }
        catch (Exception e)
        {
            throw e;
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
            if (Util.IsNullOrEmptySubitem(prms, param_name)
                && !Util.IsNullOrEmptySubitem(param_config, "mandatory"))
            {
                throw new AxWarning(string.Format("missing mandatory field: {0}", param_name));
            }

            if (Util.GetSubitem(prms, param_name) == null)
                continue;

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
                        val = fVal;
                    }
                }
                ax_class_call(ax_class, param_config["setter"], val);
                break;
            case "integer":
                if (val is string && Int32.TryParse(val, out iVal))
                {
                    val = iVal;
                }
                ax_class_call(ax_class, param_config["setter"], val);
                break;
            case "boolean":
                bool bVal;
                if (val is string)
                {
                    if (Boolean.TryParse(val, out bVal)) // "true" or "false"
                    {
                        val = bVal;
                    }
                    else if (Int32.TryParse(val, out iVal))
                    {
                        val = iVal != 0;
                    }
                    else
                    {
                        val = val.ToUpper() != "N";
                    }
                }
                else
                {
                    try {
                        val = (int)val != 0;
                    }
                    catch {}
                }
                ax_class_call(ax_class, param_config["setter"], val);
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
                        try
                        {
                            val = ((int)val != 0);
                        }
                        catch {}
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
                    bool isNext;
                    do
                    {
                        dynamic next = ax_class_call(ax_class, param_config["iterator"]);
                        isNext = (next is bool && next) || ((next is int || next is long) && next != 0);
                        if (isNext)
                        {
                            val.Add(get_values(ax_class, param_config["content"]));
                        }
                    }
                    while (isNext);
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
}
