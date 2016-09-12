@echo off
if exist amqp.exe del amqp.exe
if exist amqpmanager.exe del amqpmanager.exe

set path | find /i "Microsoft.NET\Framework\v4.0.30319" > nul || set path=c:\Windows\Microsoft.NET\Framework\v4.0.30319\;%path%

if %userdomain% == 1810tz (
    set AxaptaMock=src\AxMock.cs
    set AxaptaRef=
    set define=/define:AxMock
) else (
    set AxaptaMock=
    set AxaptaRef=/r:"c:\Program Files\Microsoft Dynamics AX\40\Client\Bin\Microsoft.Dynamics.BusinessConnectorNet.dll"
    set define=
)

set ref=/r:RabbitMQ.Client.dll /r:YamlDotNet.dll /r:fastjson.dll /r:NamedPipeWrapper.dll %AxaptaRef%

set filelist=src\AMQP.cs src\AMQPManager.cs src\AxCon.cs src\ConfigLoader.cs src\Util.cs src\Logger.cs src\dbg.cs src\Supervisor.cs src\AMQPManager.Pipe.cs src\Supervisor.Pipe.cs src\PipeMessage.cs %AxaptaMock%

:: for sending testing messages to RabbitMQ
:: if exist testSend.exe del testSend.exe
:: csc /nologo /main:TestSend testSend\testSend.cs %filelist% /lib:dll %ref%

csc /nologo /out:AMQPManager.exe /main:Supervisor /out:AMQPManager.exe %filelist% /lib:dll %define% %ref% /d:DEBUG && set buildSuccess=1

if "%1" == "clearLog" del /s /q log\* >nul 2>&1
:: if "%buildSuccess%" == "1" cmd.exe /c start AMQPManager.exe
:: build fastjson
:: @csc /t:library /out:fastjson.dll /recurse:*.cs