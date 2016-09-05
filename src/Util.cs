using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections;
using fastJSON;

public static class Util
{
    public static bool IsNullOrEmpty(object obj)
    {
        if (obj == null)
        {
            return true;
        }
        else if (obj is string && ((string)obj).Length == 0)
        {
            return true;
        }
        else if (obj is ICollection && ((ICollection)obj).Count == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    public static dynamic GetSubitem(dynamic obj, string key)
    {
        if (obj is IDictionary && obj.ContainsKey(key))
        {
            return obj[key];
        }
        else
        {
            return null;
        }
    }
    
    public static bool IsNullOrEmptySubitem(dynamic obj, string key)
    {
        return IsNullOrEmpty(GetSubitem(obj, key));
    }

    public static string ConvertString(string source, string FromEncName, string ToEncName)
    {
        string result;
        try
        {
            Encoding fromEnc = Encoding.GetEncoding(FromEncName);
            Encoding toEnc = Encoding.GetEncoding(ToEncName);
            
            byte[] FromBytes = fromEnc.GetBytes(source);
            byte[] ToBytes = Encoding.Convert(fromEnc, toEnc, FromBytes);
            Console.WriteLine("FromBytes.Length={0}",FromBytes.Length);
            Console.WriteLine("ToBytes.Length={0}",ToBytes.Length);
            result = toEnc.GetString(ToBytes);
        } catch (Exception e)
        {
            Console.WriteLine(e);
            result = null;
        }
        return result;
    }
    
    public static string md5(string input, string EncName = "windows-1251")
    {
        using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
        {
            byte[] inputBytes = Encoding.GetEncoding(EncName).GetBytes(input);
            byte[] hashBytes = md5.ComputeHash(inputBytes);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hashBytes.Length; i++)
            {
                sb.Append(hashBytes[i].ToString("x2"));
            }
            return sb.ToString();
        }
    }

    public static string RemoveVowels(string s)
    {
        string result = null;
        if (s != null)
        {
            result = Regex.Replace(s, "[aeiouy]", "", RegexOptions.IgnoreCase);
        }
        return result;
    }

    public static string CutUserHash(string s)
    {
        return Regex.Replace(s, "(user_hash(?:[^0-9,a-f]{3,10})[0-9,a-f]{10})([0-9,a-f]{22})", "$1*", RegexOptions.IgnoreCase);
    }
    
    public static string ToJSON(object obj)
    {
        string result = "{}";
        JSONParameters prms = new JSONParameters();
        prms.UseEscapedUnicode = false;
        try
        {
            result = JSON.ToJSON(obj, prms);
        }
        catch {}
        return result;
    }
}
