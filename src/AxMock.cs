using System;
using System.Collections.Generic;
using System.Threading;
using System.Runtime.InteropServices;

namespace Microsoft.Dynamics.BusinessConnectorNet {
    class Axapta {
        public static dynamic methods_config;
        public static Random random = new Random();
        public static bool isThrowExceptions;
        public static int rnd(int a, int b)
        {
            return random.Next(a, b);
        }
        public Axapta()
        {
            methods_config = AxCon.config["methods"];
            Axapta.isThrowExceptions = false;
        }

        public bool Logon(string str1, string str2, string str3, string str4)
        {
            Thread.Sleep(100 * rnd(1,20));

            if (Axapta.isThrowExceptions && rnd(0, 20) == 0)
            {
                throw new Exception("Logon exception");
            }
            if (Axapta.isThrowExceptions && Axapta.rnd(0, 10) == 0)
            {
                AxaptaObject.GenerateAccessViolationException();
            }
            return true;
        }
        public bool Logoff()
        {
            Thread.Sleep(100 * rnd(1,20));
            if (Axapta.isThrowExceptions && rnd(0, 20) == 0)
            {
                throw new Exception("Logoff exception");
            }
            if (Axapta.isThrowExceptions && Axapta.rnd(0, 10) == 0)
            {
                AxaptaObject.GenerateAccessViolationException();
            }
            return true;
        }
        public AxaptaObject CreateAxaptaObject(string objName)
        {
            return new AxaptaObject(objName);
        }
        public void Dispose()
        {}
    }

    class AxaptaObject
    {
        public string Name {get; set;}

        public AxaptaObject(string objName)
        {
            Name = objName;
        }
        public dynamic Call(string method, params dynamic[] prms)
        {
            if (method == "run")
            {
                var n = 100 * Axapta.rnd(1, 20);
                Thread.Sleep(n);
                if (Axapta.isThrowExceptions && Axapta.rnd(0, 20) == 0)
                {
                    throw new Exception("Call run() exception");
                }
            }
            if (method == "validate")
            {
                Thread.Sleep(100 * Axapta.rnd(1, 10));
                if (Axapta.isThrowExceptions && Axapta.rnd(0, 20) == 0)
                {
                    throw new Exception("Call validate() exception");
                }
            }
            int k = Axapta.rnd(0, 500);
            if (Axapta.isThrowExceptions && k == 0)
            {
                GenerateAccessViolationException();
            }
            dynamic result = "";
            var outTypeName = GetOutputValueTypeName(method);
            // Console.WriteLine("{0}:{1}:{2}", Name, method, outTypeName);
            switch (method)
            {
                case "validate":
                    result = true;
                    break;
                case "getErrorType":
                    result = 0;
                    break;
                default:
                    switch (outTypeName)
                    {
                        case "boolean":
                            result = false;
                            break;
                        case "integer":
                            result = 0;
                            break;
                        case "real":
                            result = 0.0;
                            break;
                        case "string":
                            result = "";
                            break;
                        default:
                            result = 1;
                            break;
                    }
                    break;
            }
            return result;
        }

        public dynamic Call(string method)
        {
            return Call(method, null);
        }

        public string GetOutputValueTypeName(string method)
        {
            string result = "";
            foreach (var m in Axapta.methods_config) {
                if (m.Value["class"] == Name && m.Value.ContainsKey("output"))
                {
                    foreach (var item in m.Value["output"])
                    {
                        var i = item.Value;
                        if (i.ContainsKey("getter") && i["getter"] == method) {
                            if (i.ContainsKey("type"))
                            {
                                result = i["type"];
                            }
                            else
                            {
                                result = "string";
                            }
                            break;
                        }
                    }
                    break;
                }
            }
            if (
                method == "nextItem"
                || method == "nextLocation"
                || method == "nextPrice"
                || method == "nextPrognosis"
                || method == "nextOutputLine"
                || method == "nextOutputLine1"
                || method == "nextOutputLine2"
                || method == "nextOutputLine3"
                || method == "nextOutputLine4"
                || method == "nextOutputLine5"
                || method == "nextOutputLineGroup"
                || method == "nextLine"
                || method == "nextTransport"
                || method == "next"
                || method == "getAnalogExists"
                || method == "getQuotationOnly"
                || method == "getApplAreaMandatory"
                || method == "getPickOnly"
                || method == "getIsCritical"
                || method == "getIsSpecAction"
                || method == "getForecastAvailability"
                )
            {
                result = "boolean";
            }
            return result;
        }

        public void Dispose()
        {}

        public static void GenerateAccessViolationException()
        {
            var ptr = new IntPtr(42);
            Marshal.StructureToPtr(42, ptr, true);
        }
    }

    class ServerUnavailableException : Exception
    {
    }
    class SessionTerminatedException : Exception
    {
    }
    class BusinessConnectorInstanceInvalid : Exception
    {
    }
}