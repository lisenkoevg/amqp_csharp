using System;
using System.Collections.Generic;

namespace Microsoft.Dynamics.BusinessConnectorNet {
    class Axapta {
        public static dynamic methods_config;
        
        public Axapta(dynamic methods_conf = null)
        {
            methods_config = methods_conf;
        }
       
        public bool Logon(string str1, string str2, string str3, string str4)
        {
            return true;
        }
        public bool Logoff()
        {
            return true;
        }
        public AxaptaObject CreateAxaptaObject(string objName)
        {
            return new AxaptaObject(objName);
        }
    }
    
    class AxaptaObject
    {
        public string Name {get; set;}
        
        public AxaptaObject(string objName)
        {
            Name = objName;
        }
        
        public dynamic Call(string method, params string[] prms)
        {
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
        
        public string GetOutputValueTypeName(string method)
        {
            string result = "";
            foreach (var m in Axapta.methods_config) {
                if (m.Value["class"] == Name)
                {
                    foreach (var i in m.Value["output"])
                    {
                        if (i.ContainsKey("getter") && i["getter"] == method) {
                            result = i["type"];
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
    }
}