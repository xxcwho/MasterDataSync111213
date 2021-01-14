@echo off
SETLOCAL ENABLEDELAYEDEXPANSION 

set PROJECT_DIR=%~dp0..
set LIB_DIR=%PROJECT_DIR%\lib
set CONF_DIR=%PROJECT_DIR%\conf
set LOG_DIR=%PROJECT_DIR%\log

set JAVA_OPTS=-Xms4096m -Xmx10240m

if exist %PROJECT_DIR%\is_running (
    echo "Process is running,exit..."
    exit 3
)
echo > %PROJECT_DIR%\is_running

for /r %LIB_DIR% %%i in (*) do (
	set CLASSPATH=!CLASSPATH!;%%i
)
set CLASSPATH=%CONF_DIR%;%CLASSPATH%

java %JAVA_OPTS% -cp %CLASSPATH% -Dlog.dir=%LOG_DIR% com.chinacscs.MasterDataSyncApplication StageLoad %*

if exist %PROJECT_DIR%\is_running (
    del %PROJECT_DIR%\is_running
)