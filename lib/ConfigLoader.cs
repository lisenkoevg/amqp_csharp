using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using YamlDotNet.Serialization;
using fastJSON;

/*
Comments for fields.yaml:

Fix setter:
application_area_mandatory:
    getter: getApplAreaMandatory
    setter: getApplAreaMandatory
    
Add single quotes (need for YamlDotNet.Serialization.Deserializer):
segment:
...
    description: 'Сегмент продукции (условный код: "development", "production" итп.)'
    
*/

class ConfigLoader
{
    public static Dictionary<string,object> Load(string path)
    {
        Dictionary<string,dynamic> result = new Dictionary<string,object>()
        {
            {"settings", LoadFile(path + "/settings.yaml")},
            {"enums",  LoadFile(path + "/enums.yaml")},
            {"methods", new Dictionary<string,object>()}
        };
        dynamic fields =  LoadFile(path + "/fields.yaml");
        dynamic common =  LoadFile(path + "/common.yaml");
        DirectoryInfo dir = new DirectoryInfo(path + "/methods/");
        FileInfo[] files = dir.GetFiles("*.yaml", SearchOption.TopDirectoryOnly);
        foreach (FileInfo f in files) {
            result["methods"][f.Name.Replace(f.Extension, "")] =  LoadFile(f.FullName);
        }
        
        h2a(result["methods"], "methods", fields);

        Dictionary<string,List<string>> legacy_enums = new Dictionary<string,List<string>>();
        foreach(string item in result["enums"].Keys)
        {
            legacy_enums[item] = new List<string>();
            legacy_enums[item + "_rus"] = new List<string>();
            foreach (dynamic elem in result["enums"][item]["elements"])
            {
                legacy_enums[item].Add(elem["name"]);
                legacy_enums[item + "_rus"].Add(elem["title"]);
            }
        }
        result["enums"] = legacy_enums;
        return result;
    }

    public static dynamic LoadFile(string path)
    {
        dynamic result;
        
        var deserializer = new Deserializer();
        // Load Yaml
        var yamlObject = deserializer.Deserialize(new StreamReader(path));
        // then serialize to JSON
        var serializer = new Serializer(SerializationOptions.JsonCompatible);
        var str = new StringWriter();
        serializer.Serialize(str, yamlObject);
        // and then to .NET object (nested Dictionaries and Lists)
        result = JSON.Parse(str.ToString());
        return result;
    }
    
    // public static dynamic tmp = new Dictionary<string,dynamic>() {
        // {"input", ""},
        // {"output", ""},
        // {"content", ""},
    // };
    public static object h2a(dynamic a, string key, dynamic fields)
    {
        if (( key == "input") || ( key == "output") || ( key == "content"))
        {
            Debug.Assert(a is IList);
            dynamic h = new Dictionary<string,dynamic>();
            
            for (int i = 0; i < a.Count; i++){
                if (a[i].ContainsKey("field"))
                {
                    foreach (string f in fields[a[i]["field"]].Keys)
                    {
                        // Console.WriteLine("{0} {1}", f, fields[a[i]["field"]][f]);
                        if (!a[i].ContainsKey(f))
                        {
                            a[i].Add(f, fields[a[i]["field"]][f]);
                        }
                    }

                }
                a[i].Add("position", Convert.ToString(i + 1));
                if (!a[i].ContainsKey("name"))
                {
                    continue;
                }
                h.Add(a[i]["name"], a[i]);
                // a = h; //doesn't work (dynamic,by val,indexer?)
            }
            // if ( key == "input") { tmp["input"] = h; }
            // if ( key == "output") { tmp["output"] = h; }
            // if ( key == "content") { tmp["content"] = h; }
        }

        if (a is IDictionary)
        {
            foreach(string subkey in a.Keys)
            {
                if (a[subkey] is ICollection)
                {
                    h2a(a[subkey], subkey, fields);
                }
            }
            // if (a.ContainsKey("input")) { a["input"] = tmp["input"]; }
            // if (a.ContainsKey("output")) { a["output"] = tmp["output"]; }
            
        } else if (a is IList)
        {
            for(int i = 0; i < a.Count; i++)
            {
                if (a[i] is ICollection)
                {
                    h2a(a[i], Convert.ToString(i), fields);
                    // if (a[i].ContainsKey("content")) { a[i]["content"] = tmp["content"]; }
                }
            }
        }
        return a;
    }        
}
