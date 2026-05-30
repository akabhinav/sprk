@echo off
REM Start a MiniYarn NodeManager. Required: name.
REM   scripts\start-nm.cmd <nodeName> [rmHost] [rmPort] [host] [port] [cores] [memMB]
setlocal
set NAME=%1
if "%NAME%"=="" (
  echo Usage: scripts\start-nm.cmd ^<nodeName^> [rmHost] [rmPort] [host] [port] [cores] [memMB]
  exit /b 1
)
set RMHOST=%2
if "%RMHOST%"=="" set RMHOST=127.0.0.1
set RMPORT=%3
if "%RMPORT%"=="" set RMPORT=8032
set HOST=%4
if "%HOST%"=="" set HOST=127.0.0.1
set PORT=%5
if "%PORT%"=="" set PORT=0
set CORES=%6
if "%CORES%"=="" set CORES=4
set MEM=%7
if "%MEM%"=="" set MEM=4096

call mvnw.cmd -q -pl miniyarn -am install -DskipTests
call mvnw.cmd -q -pl miniyarn exec:java ^
  -Dexec.mainClass=com.miniyarn.nm.NodeManager ^
  -Dexec.args="%NAME% %RMHOST% %RMPORT% %HOST% %PORT% %CORES% %MEM%"
endlocal
