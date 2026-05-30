@echo off
REM Start the MiniYarn ResourceManager. Defaults: 127.0.0.1:8032.
REM   scripts\start-rm.cmd [host] [port]
setlocal
set HOST=%1
if "%HOST%"=="" set HOST=127.0.0.1
set PORT=%2
if "%PORT%"=="" set PORT=8032

call mvnw.cmd -q -pl miniyarn -am install -DskipTests
call mvnw.cmd -q -pl miniyarn exec:java ^
  -Dexec.mainClass=com.miniyarn.rm.ResourceManager ^
  -Dexec.args="%HOST% %PORT%"
endlocal
