@echo off
REM Submit an example to a running MiniYarn cluster.
REM Requires: RM started on RMHOST:RMPORT, at least one NodeManager.
REM   scripts\submit-yarn.cmd C:\data\book.txt
REM Env: RMHOST (127.0.0.1), RMPORT (8032), EXECUTORS (2), CORES (2), MEM (256)

setlocal
if "%RMHOST%"=="" set RMHOST=127.0.0.1
if "%RMPORT%"=="" set RMPORT=8032
if "%EXECUTORS%"=="" set EXECUTORS=2
if "%CORES%"=="" set CORES=2
if "%MEM%"=="" set MEM=256
set INPUT=%1
if "%INPUT%"=="" set INPUT=README.txt

call mvnw.cmd -q -pl minispark-examples -am install -DskipTests
call mvnw.cmd -q -pl minispark-examples exec:java ^
  -Dexec.mainClass=com.minispark.examples.WordCount ^
  -Dexec.args="%INPUT%" ^
  -Dminispark.master=miniyarn://%RMHOST%:%RMPORT% ^
  -Dminispark.executor.instances=%EXECUTORS% ^
  -Dminispark.executor.cores=%CORES% ^
  -Dminispark.executor.memoryMB=%MEM%
endlocal
