@echo off
if exist amqp.exe del amqp.exe

set path=c:\Windows\Microsoft.NET\Framework64\v4.0.30319\;%path%
:: set path=c:\Windows\Microsoft.NET\Framework64\v3.5\;%path%

:: debug
set AxaptaMock=lib\AxMock.cs

:: production
:: set AxaptaRef=/r:"C:\...\Microsoft.Dynamics.BusinessConnectorNet.dll"

set filelist=AMQP.cs lib\ConfigLoader.cs lib\AxCon.cs lib\Util.cs lib\dbg.cs %AxaptaMock%
set ref=/r:RabbitMQ.Client.dll /r:YamlDotNet.dll /r:fastjson.dll %AxaptaRef%

csc /nologo %filelist% /main:AMQP /d:DEBUG %ref%

:: build fastjson
:: @csc /t:library /out:fastjson.dll /recurse:*.cs
