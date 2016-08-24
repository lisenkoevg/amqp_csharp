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

item_info.yaml
output
    discontinued
    regulations
 declared type is boolean, but returns int

 search_item_name_h.yaml
     application_area_mandatory Int32
    reserve_control Int32
    special_offer Int32
*/

class ConfigLoader
{
    public static Dictionary<string,object> Load(string path)
    {
        DateTime lastModified;
        
        Dictionary<string,dynamic> result = new Dictionary<string,object>()
        {
            {"enums",  LoadFile(path + "/enums.yaml")},
            {"methods", new Dictionary<string,object>()}
        };
        lastModified = File.GetLastWriteTime(path + "/enums.yaml");
        dynamic fields =  LoadFile(path + "/fields.yaml");
        lastModified = File.GetLastWriteTime(path + "/fields.yaml") > lastModified
            ? File.GetLastWriteTime(path + "/fields.yaml")
            : lastModified;
        // dynamic common =  LoadFile(path + "/common.yaml");
        DirectoryInfo dir = new DirectoryInfo(path + "/methods/");
        FileInfo[] files = dir.GetFiles("*.yaml", SearchOption.TopDirectoryOnly);
        foreach (FileInfo f in files) {
            result["methods"][f.Name.Replace(f.Extension, "")] =  LoadFile(f.FullName);
            lastModified = File.GetLastWriteTime(f.FullName) > lastModified
                ? File.GetLastWriteTime(f.FullName)
                : lastModified;
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
        result["lastModified"] = lastModified;
        return result;
    }

    public static dynamic LoadFile(string path)
    {
        dynamic result;

        var deserializer = new Deserializer();
        dynamic yamlObject = new Object();
        var sr = new StreamReader(path);
        try
        {
            // Dictionary<object,object>
            yamlObject = deserializer.Deserialize(sr);
        }
        catch (Exception e)
        {
            sr.Dispose();
            throw new Exception(string.Format("Exception while deserializing file {0}\n{1}", path, e.Message));
        }
        sr.Dispose();
        var serializer = new Serializer(SerializationOptions.JsonCompatible);
        var str = new StringWriter();
        serializer.Serialize(str, yamlObject);
        // Dictionary<string,object>
        result = JSON.Parse(str.ToString());
        return result;
    }

    public static object h2a(dynamic a, string key, dynamic fields, dynamic parent = null)
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
            }
            parent[key] = h;
        }

        if (a is IDictionary)
        {
            dynamic keys = new List<dynamic>(a.Keys);
            foreach(string subkey in keys)
            {
                if (a[subkey] is ICollection)
                {
                    h2a(a[subkey], subkey, fields, a);
                }
            }

        } else if (a is IList)
        {
            for(int i = 0; i < a.Count; i++)
            {
                if (a[i] is ICollection)
                {
                    h2a(a[i], Convert.ToString(i), fields, a);
                }
            }
        }
        return a;
    }
}
