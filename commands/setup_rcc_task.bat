@echo off
:: Automatically find the full path of rcc.exe
for /f "tokens=*" %%i in ('where rcc') do set RCC_PATH=%%i

:: Check if RCC_PATH is found
if "%RCC_PATH%"=="" (
    echo RCC not found. Please ensure it is installed and in PATH.
    exit /b
)

:: Get the directory where this script is located (assumes robot.yaml is in ../)
set "PROJECT_PATH=%~dp0"
set "PARENT_PATH=%PROJECT_PATH:~0,-1%"
for %%A in ("%PARENT_PATH%") do set "PARENT_PATH=%%~dpA"

:: Define the task name
set TASK_NAME=RunRCCCommand

:: Define the batch script content to execute rcc
setlocal enabledelayedexpansion
set BATCH_FILE=%TEMP%\run_rcc.bat
(
    echo @echo off
    echo cd /d "%PARENT_PATH%"
    echo "%RCC_PATH%" run --robot "%PARENT_PATH%robot.yaml"
) > "%BATCH_FILE%"

:: Create a scheduled task to run every 10 minutes
schtasks /create /tn "%TASK_NAME%" /tr "%BATCH_FILE%" /sc minute /mo 10 /f

echo Scheduled task "%TASK_NAME%" created successfully!
