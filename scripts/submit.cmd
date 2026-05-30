@echo off
REM Run an example in DISTRIBUTED mode: the driver launches real executor JVMs
REM that connect back over TCP (rpc.mode=netty). Usage:
REM   scripts\submit.cmd C:\data\book.txt
REM Optional env vars: EXECUTORS (default 2), CORES (default 2)

setlocal
if "%EXECUTORS%"=="" set EXECUTORS=2
if "%CORES%"=="" set CORES=2
set INPUT=%1
if "%INPUT%"=="" set INPUT=README.txt

call mvnw.cmd -q -pl minispark-examples -am install -DskipTests
call mvnw.cmd -q -pl minispark-examples exec:java ^
  -Dexec.mainClass=com.minispark.examples.WordCount ^
  -Dexec.args="%INPUT%" ^
  -Dminispark.rpc.mode=netty ^
  -Dminispark.executor.instances=%EXECUTORS% ^
  -Dminispark.executor.cores=%CORES%
endlocal
