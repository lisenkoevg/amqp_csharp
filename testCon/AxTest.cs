using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Collections.Generic;
using System.Collections;

class AxTest
{
	// public static Microsoft.Dynamics.BusinessConnectorNet.Axapta ax; 
    
	public static void Main()
	{
        T();
        Console.WriteLine("before exit");
        Console.ReadLine();
	}
    
    public static void T()
    {
            Console.WriteLine("before create");
            Console.ReadLine();

            var ax = new Microsoft.Dynamics.BusinessConnectorNet.Axapta();
            Console.WriteLine("before logon");
            Console.ReadLine();
            ax.Logon("rba", "ru", "192.168.3.120:2714", "");
            Console.WriteLine("before logoff");
            Console.ReadLine();
            
            try
            {
                ax.Logoff();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            
            Console.WriteLine("before dispose");
            Console.ReadLine();
            try
            {
                ax.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            
            GC.Collect(); 
            GC.WaitForPendingFinalizers();
            Console.WriteLine("before null");
            Console.ReadLine();
            ax = null;
            GC.Collect(); 
            GC.WaitForPendingFinalizers();        
    }
/*
	public static void Test4()
	{
        var obj = ax.CreateAxaptaObject("cmpECommerce_InventSearchStock");
        obj.Call("clear");
        obj.Call("setcmpHashCode", "_a4b232a0f80f8edb81fc34edd9989d2d");
        obj.Call("setQuery", "qwerqwer");
        obj.Call("init");
        obj.Call("validate");
        obj.Call("run");
        var err_type = obj.Call("getErrorType");
        Console.WriteLine("{0}, {1}", err_type.GetType(), err_type);
        
	}
	
    public static void Test3()
    {
        var obj = ax.CreateAxaptaObject("cmpECommerceTimestamp");
        obj.Call("clear");
        obj.Call("setcmpHashCode", "a4b232a0f80f8edb81fc34edd9989d2d");
        obj.Call("init");
        obj.Call("validate");
        obj.Call("run");
        
        var ts = obj.Call("getTimestamp");
        Console.WriteLine("{0}, {1}", ts.GetType(), ts);
        
        // var err_type = obj.Call("getErrorType");
        // Console.WriteLine("{0}, {1}", err_type.GetType(), err_type);

    }
	
    public static void Test1()
    {
        var obj = ax.CreateAxaptaObject("cmpECommerceInventTableInfo");
        obj.Call("clear");
        obj.Call("setcmpHashCode", "a4b232a0f80f8edb81fc34edd9989d2d");
        obj.Call("setItemid", "qwer203866");//int32
        obj.Call("init");//int32
        obj.Call("validate"); //boolean, check userhash
        obj.Call("run"); // Int32
        Console.WriteLine(obj.Call("getErrorType"));
        Console.WriteLine(obj.Call("getErrorDescription"));
        
        //Console.WriteLine("res={0}, type={1}", res, res.GetType());
        
        var Volume = obj.Call("getVolume");
        var GrossWeight = obj.Call("getGrossWeight");
        var PackingType = obj.Call("getPackingType");
        var Requlations = obj.Call("getRequlations");
        var Discontinued = obj.Call("getDiscontinued");
        var QtyInPack = obj.Call("getQtyInPack");
        var IPGInventName = obj.Call("getIPGInventName");
        var TxtInvent = obj.Call("getTxtInvent");
        var QuotationProcessMinQty = obj.Call("getQuotationProcessMinQty");
        var QuotationProcessMinAmount = obj.Call("getQuotationProcessMinAmount");
        var ECCNControl = obj.Call("getECCNControl");

        Console.WriteLine(Volume);
        Console.WriteLine(GrossWeight);
        Console.WriteLine(PackingType);
        Console.WriteLine(Requlations);
        Console.WriteLine(Discontinued);
        Console.WriteLine(QtyInPack);
        Console.WriteLine(IPGInventName);
        Console.WriteLine(TxtInvent);
        Console.WriteLine(QuotationProcessMinQty);
        Console.WriteLine(QuotationProcessMinAmount);
        Console.WriteLine(ECCNControl);			
        
        Console.WriteLine(Volume.GetType());
        Console.WriteLine(GrossWeight.GetType());
        Console.WriteLine(PackingType.GetType());
        Console.WriteLine(Requlations.GetType());
        Console.WriteLine(Discontinued.GetType());
        Console.WriteLine(QtyInPack.GetType());
        Console.WriteLine(IPGInventName.GetType());
        Console.WriteLine(TxtInvent.GetType());
        Console.WriteLine(QuotationProcessMinQty.GetType());
        Console.WriteLine(QuotationProcessMinAmount.GetType());
        Console.WriteLine(ECCNControl.GetType());		    
    }
*/
}