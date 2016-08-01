@echo off
if exist amqp.exe del amqp.exe

set path=c:\Windows\Microsoft.NET\Framework\v4.0.30319\;%path%
:: set path=c:\Windows\Microsoft.NET\Framework64\v3.5\;%path%

if %userdomain% == 1810tz (
    set AxaptaMock=lib\AxMock.cs
    set AxaptaRef=
) else (
    set AxaptaMock=
    set AxaptaRef=/r:"c:\Program Files\Microsoft Dynamics AX\40\Client\Bin\Microsoft.Dynamics.BusinessConnectorNet.dll"
)

set filelist=AMQP.cs lib\AxCon.cs lib\ConfigLoader.cs lib\Util.cs lib\dbg.cs %AxaptaMock%
set ref=/r:RabbitMQ.Client.dll /r:YamlDotNet.dll /r:fastjson.dll %AxaptaRef%
csc /nologo /lib:dll %filelist% /main:AMQP /d:DEBUG %ref%


:: for sending testing messages to RabbitMQ
if exist testSend.exe del testSend.exe
csc /nologo /lib:dll testSend.cs lib\ConfigLoader.cs lib\Util.cs lib\dbg.cs /main:TestSend %ref%

:: build fastjson
:: @csc /t:library /out:fastjson.dll /recurse:*.cs