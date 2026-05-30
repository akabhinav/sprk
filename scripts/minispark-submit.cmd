@echo off
REM minispark-submit — the MiniSpark analogue of spark-submit (Windows).
REM
REM   scripts\minispark-submit.cmd ^
REM       --class com.minispark.examples.WordCount ^
REM       --master netty --num-executors 2 --executor-cores 2 ^
REM       C:\data\book.txt
REM
REM Set SKIP_BUILD=1 to skip the Maven build.
setlocal
cd /d "%~dp0.."

if not "%SKIP_BUILD%"=="1" (
  call mvnw.cmd -q -pl minispark-examples -am install -DskipTests
)

set CP_FILE=target\minispark-classpath.txt
if not exist target mkdir target
call mvnw.cmd -q -pl minispark-examples dependency:build-classpath -Dmdep.outputFile="%CD%\%CP_FILE%" >nul
set /p DEP_CP=<%CP_FILE%
set MODULE_CP=minispark-rpc\target\classes;miniyarn\target\classes;minispark-core\target\classes;minispark-sql\target\classes;minispark-examples\target\classes

java -cp "%MODULE_CP%;%DEP_CP%" com.minispark.deploy.MiniSparkSubmit %*
endlocal
