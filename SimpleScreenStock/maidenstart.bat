@echo off
chcp 936 >nul
setlocal
title maidenstart - A股智能选股 一键启动
cd /d "%~dp0"

set PORT=8501
set URL=http://127.0.0.1:%PORT%

rem ============ 子命令: install / stop ============
if /i "%~1"=="install" goto do_install
if /i "%~1"=="stop" goto do_stop

echo ==================================================
echo   maidenstart · A股智能选股（AKShare + Streamlit）
echo ==================================================
echo.

rem ---- 1. 定位 Python ----
set PYCMD=
where python >nul 2>nul && set PYCMD=python
if not defined PYCMD (
    where py >nul 2>nul && set PYCMD=py -3
)
if not defined PYCMD (
    echo [错误] 未找到 Python 3。
    echo        请先安装 Python 3.9+ 并勾选 "Add python.exe to PATH"，然后重试。
    pause
    exit /b 1
)

rem ---- 2. 依赖自检，缺失则自动安装 ----
echo [1/3] 检查运行依赖...
%PYCMD% -c "import streamlit, akshare, pandas, requests" >nul 2>nul
if errorlevel 1 call :do_install
if errorlevel 1 (
    echo [错误] 依赖安装失败，请检查网络后重试。
    pause
    exit /b 1
)
echo       依赖 OK。

rem ---- 3. 端口检测与启动 ----
echo [2/3] 检查服务状态...
netstat -ano | findstr "LISTENING" | findstr ":%PORT% " >nul 2>nul
if %errorlevel%==0 goto already_running

echo       启动 Streamlit 服务（最小化窗口，关闭它即停止服务）...
start "maidenstart-streamlit" /min cmd /c "cd /d %~dp0 && %PYCMD% -m streamlit run app.py --server.headless true --server.address 127.0.0.1 --server.port %PORT% --browser.gatherUsageStats false"

echo       等待服务就绪...
set /a TRIES=0
:wait_loop
ping -n 2 127.0.0.1 >nul
netstat -ano | findstr "LISTENING" | findstr ":%PORT% " >nul 2>nul
if %errorlevel%==0 goto already_running
set /a TRIES+=1
if %TRIES% GEQ 25 goto timeout_msg
goto wait_loop

:timeout_msg
echo [警告] 25 秒内服务未就绪，请查看最小化控制台窗口中的报错信息。
echo        常见原因：端口被占用、依赖不完整。可用 "maidenstart install" 修复依赖。

:already_running
rem ---- 4. 打开浏览器 ----
echo [3/3] 打开页面 %URL%
start "" "%URL%"

echo.
echo ==================================================
echo   启动完成！页面地址: %URL%
echo   停止服务: 关闭最小化控制台窗口，或运行 maidenstart stop
echo   关闭本窗口不会停止服务。
echo ==================================================
echo.
endlocal
pause
exit /b 0

rem ============ 依赖安装子程序 ============
:do_install
echo [安装] 正在安装依赖 requirements.txt（首次约 1-3 分钟，请稍候）...
%PYCMD% -m pip install -r requirements.txt --disable-pip-version-check
if errorlevel 1 (
    echo [重试] 默认源失败，改用清华镜像...
    %PYCMD% -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --disable-pip-version-check
)
exit /b %errorlevel%

rem ============ 停止服务子程序 ============
:do_stop
echo 正在停止 %PORT% 端口的 Streamlit 服务...
set FOUND=0
for /f "tokens=5" %%P in ('netstat -ano ^| findstr "LISTENING" ^| findstr ":%PORT% "') do (
    taskkill /f /pid %%P >nul 2>nul
    if not errorlevel 1 set FOUND=1
)
if "%FOUND%"=="0" (
    echo [提示] 未发现正在运行的服务。
) else (
    echo [完成] 服务已停止。
)
endlocal
pause
exit /b 0
