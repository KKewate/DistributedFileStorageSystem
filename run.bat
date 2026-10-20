@echo off
REM Compiles the project, starts 3 DataNodes + the NameNode on localhost, then opens the Client.
REM Commands:  append <file> <text>   |   read <file>   |   shutdown
cd /d "%~dp0"
if not exist bin mkdir bin
if not exist run mkdir run

javac -d bin src\*.java
if errorlevel 1 (
    echo.
    echo Compilation failed. Make sure a JDK is installed ^(javac -version^).
    pause
    exit /b 1
)

cd run
REM "cmd /k" keeps each window open if its program crashes, so you can read the error.
start "DataNode 65530" cmd /k java -cp ..\bin DataNode 65530
start "DataNode 65531" cmd /k java -cp ..\bin DataNode 65531
start "DataNode 65532" cmd /k java -cp ..\bin DataNode 65532
start "NameNode" cmd /k java -cp ..\bin NameNode

REM Wait (up to ~30 seconds) until the NameNode and all 3 DataNodes accept connections.
set /a tries=0
:waitloop
powershell -NoProfile -Command "foreach($p in 5558,65530,65531,65532){ try{ $c=New-Object Net.Sockets.TcpClient('127.0.0.1',$p); $c.Close() } catch { exit 1 } }; exit 0"
if not errorlevel 1 goto ready
set /a tries+=1
if %tries% geq 30 goto failed
timeout /t 1 >nul
goto waitloop

:failed
echo.
echo The NameNode / DataNodes did not start. Look at the NameNode and DataNode
echo windows for the error message ^(they stay open^) and send it to me.
pause
exit /b 1

:ready
echo DFS running on localhost ^(NameNode :5558, DataNodes :65530-65532^)
echo Type:  append notes.txt hello world   or   read notes.txt   or   shutdown
java -cp ..\bin Client
echo.
echo Client closed. Close the DataNode/NameNode windows to stop everything.
pause
