@echo off
SETLOCAL ENABLEDELAYEDEXPANSION 

set PROJECT_DIR=%~dp0..
set LIB_DIR=%PROJECT_DIR%\lib
set CONF_DIR=%PROJECT_DIR%\conf
set LOG_DIR=%PROJECT_DIR%\log

set JAVA_OPTS=-Xms2048m -Xmx10240m
set SYNC_STEP=2

if exist %PROJECT_DIR%\is_running (
    echo "Process is running,exit..."
    exit 3
)
echo > %PROJECT_DIR%\is_running

for /r %LIB_DIR% %%i in (*) do (
	set CLASSPATH=!CLASSPATH!;%%i
)
set CLASSPATH=%CONF_DIR%;%CLASSPATH%

if not %SYNC_STEP%==3 (
    java %JAVA_OPTS% -cp %CLASSPATH% -Dlog.dir=%LOG_DIR% com.chinacscs.MasterDataSyncApplication DownloadFromSftp %*
    if not %errorlevel%==0 (
        del %PROJECT_DIR%\is_running
        exit -1
    )
    if %SYNC_STEP%==1 (
        del %PROJECT_DIR%\is_running
        echo "Only download file from sftp!"
        exit 0
    )
)

java %JAVA_OPTS% -cp %CLASSPATH% -Dlog.dir=%LOG_DIR% com.chinacscs.MasterDataSyncApplication StageLoad %*

set STATUS=%errorlevel%

if %STATUS%==-1 (
    del %PROJECT_DIR%\is_running
)

if not %STATUS%==0 (
    exit -1
)

java %JAVA_OPTS% -cp %CLASSPATH% -Dlog.dir=$LOG_DIR com.chinacscs.MasterDataSyncApplication TargetLoad %*

set STATUS=%errorlevel%

if %STATUS%==-1 (
    del %PROJECT_DIR%\is_running
)

if not %STATUS%==0 (
    exit -1
)

if exist %PROJECT_DIR%\is_running (
    del %PROJECT_DIR%\is_running
)