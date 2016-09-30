using System;
using System.IO;
using System.Threading;
using System.Security.Permissions;

public partial class AMQPManager
{
    public static string configFile = @"config\AMQPManager.yaml";
    public static string logDir = Path.GetDirectoryName(configFile) + "\\" + "..\\log";
    public static int managersCount = 1;
    private static string cmdConfigFile;
    private static int maxWorkersCount = 30;
    private static int amqpInitTimeout = 15000;
    private static int axconInitTimeout = 30000;
    private static int axconRequestTimeout = 60000;
    private static int workersCheckPeriod = 15000;
    private static bool axconUseClassPool = false;
    private static int startupWorkersCount = 4;
    private static bool configAutoReload = false;
    private static FileSystemWatcher configFileWatcher;
    
    public static void Configure()
    {
        dynamic conf = LoadConfig();
        if (conf == null)
            return;
        int parsedValue = 0;
        if (conf.ContainsKey("maxWorkersCount") && Int32.TryParse(conf["maxWorkersCount"], out parsedValue))
        {
            maxWorkersCount = parsedValue;
        }
        if (conf.ContainsKey("managersCount") && Int32.TryParse(conf["managersCount"], out parsedValue))
        {
            if (parsedValue > 0 && parsedValue < 30)
            {
                managersCount = parsedValue;
            }
        }
        if (conf.ContainsKey("startupWorkersCount") && Int32.TryParse(conf["startupWorkersCount"], out parsedValue))
        {
            if (parsedValue > 0 && parsedValue <= maxWorkersCount)
            {
                startupWorkersCount = parsedValue;
            }
        }
        if (conf.ContainsKey("amqpInitTimeout") && Int32.TryParse(conf["amqpInitTimeout"], out parsedValue))
        {
            amqpInitTimeout = parsedValue;
        }
        if (conf.ContainsKey("axconInitTimeout") && Int32.TryParse(conf["axconInitTimeout"], out parsedValue))
        {
            axconInitTimeout = parsedValue;
        }
        if (conf.ContainsKey("axconRequestTimeout") && Int32.TryParse(conf["axconRequestTimeout"], out parsedValue))
        {
            axconRequestTimeout = parsedValue;
        }
        if (conf.ContainsKey("axconUseClassPool") && Int32.TryParse(conf["axconUseClassPool"], out parsedValue))
        {
            axconUseClassPool = parsedValue != 0;
        }
        if (conf.ContainsKey("configAutoReload") && Int32.TryParse(conf["configAutoReload"], out parsedValue))
        {
            configAutoReload = parsedValue != 0;
        }
        if (conf.ContainsKey("logDir") && conf["logDir"] != null)
        {
            conf["logDir"] = conf["logDir"].Trim();
            if (conf["logDir"].Length > 0)
            {
                logDir = Path.IsPathRooted(conf["logDir"]) ? conf["logDir"] : Path.GetDirectoryName(configFile) + "\\" + conf["logDir"];
            }
        }
        if (configAutoReload)
        {
            AddConfigDirectoryWatcher();
        }
    }

    public static object LoadConfig()
    {
        object result = null;
        cmdConfigFile = Supervisor.GetArg("config");
        if (cmdConfigFile != null)
        {
            configFile = cmdConfigFile;
        }
        if (File.Exists(configFile))
        {
            try
            {
                result = ConfigLoader.LoadFile(configFile);
            }
            catch (Exception e)
            {
                try {Console.Error.WriteLine(e.Message);} catch {}
                Environment.Exit(1);
            }
        }
        else
        {
            if (cmdConfigFile != null)
            {
                try {Console.Error.WriteLine("Config file '{0}' not found.", configFile);} catch {}
                Environment.Exit(1);
            }
            else
            {
                CreateConfigFile();
            }
        }
        return result;
    }

    public static void CreateConfigFile()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(configFile));
            using (var sw = new StreamWriter(configFile))
            {
                sw.WriteLine("# timeouts in milliseconds");
                sw.WriteLine("amqpInitTimeout: {0}", amqpInitTimeout);
                sw.WriteLine("axconInitTimeout: {0}", axconInitTimeout);
                sw.WriteLine("axconRequestTimeout: {0}", axconRequestTimeout);
                sw.WriteLine("startupWorkersCount: {0}", startupWorkersCount);
                sw.WriteLine("managersCount: {0}", managersCount);
                sw.WriteLine("maxWorkersCount: {0}", maxWorkersCount);
                sw.WriteLine("# 1/0");
                sw.WriteLine("axconUseClassPool: {0}", axconUseClassPool ? "1" : "0");
                sw.WriteLine("# logDir relative to config file or absolute if started with \\ or <letter>:");
                sw.WriteLine("logDir: {0}", logDir.Substring(Path.GetDirectoryName(configFile).Length + 1));
                sw.WriteLine("# 1/0");
                sw.WriteLine("configAutoReload: {0}", configAutoReload ? "1" : "0");
            }
        }
        catch (Exception e)
        {
            try { Console.Error.WriteLine(e); } catch {};
            Environment.Exit(1);
        }
    }

    public static void PrepareLogDir()
    {
        string testFile = logDir + "\\" + Path.GetRandomFileName();
        try
        {
            Directory.CreateDirectory(logDir);
            using (var sw = new StreamWriter(testFile))
            {
                sw.WriteLine("Check if writable");
            }
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Can't create or write to log dir [{0}]\n{1}", logDir, e);
            Environment.Exit(1);
        }
        finally
        {
            try { File.Delete(testFile); } catch {}
        }
    }
    
    public static void AddConfigDirectoryWatcher()
    {
        configFileWatcher = new FileSystemWatcher(AxCon.configDir, "*.yaml");
        configFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;
        configFileWatcher.IncludeSubdirectories = true;
        Action<object> ReloadConfig = (obj) => {
            var ea = (System.IO.FileSystemEventArgs)obj;
            if (Path.GetFullPath(configFile) == Path.GetFullPath(ea.FullPath)) return;
            dbg.fa("WatchConfigDirectory AutoReload " + ea.ChangeType + " " + ea.FullPath);
            infoMsg = AxCon.LoadConfig();
        };
        FileSystemEventHandler OnChanged = (source, ea) => {
            ReloadConfig(ea);
        };
        
        RenamedEventHandler OnRenamed = (source, ea) => {
            ReloadConfig(ea);
        };
        configFileWatcher.Changed += OnChanged;
        // configFileWatcher.Created += OnChanged;
        configFileWatcher.Deleted += OnChanged;
        configFileWatcher.Renamed += OnRenamed;
        
        configFileWatcher.EnableRaisingEvents = true;
    }
}