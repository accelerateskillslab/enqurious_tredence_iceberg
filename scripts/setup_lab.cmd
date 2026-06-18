@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "REPO_ROOT=%~dp0.."
set "VENV_DIR=%REPO_ROOT%\.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "REQUIREMENTS=%REPO_ROOT%\requirements.txt"
set "PYTHON311_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"

echo Iceberg Lab Python Setup
echo.

if not exist "%REQUIREMENTS%" (
    echo ERROR: requirements.txt was not found at "%REQUIREMENTS%".
    exit /b 1
)

echo Checking Python...
call :EnsurePython || exit /b 1

echo.
echo Creating virtual environment...
if not exist "%PYTHON_EXE%" (
    %PYTHON_CMD% -m venv "%VENV_DIR%"
    if errorlevel 1 exit /b 1
) else (
    echo Virtual environment already exists.
)

echo.
echo Installing requirements...
"%PYTHON_EXE%" -m pip install --upgrade pip
if errorlevel 1 exit /b 1

"%PYTHON_EXE%" -m pip install -r "%REQUIREMENTS%"
if errorlevel 1 exit /b 1

echo.
echo Setup complete.
echo Virtual environment: "%VENV_DIR%"
echo Requirements installed from: "%REQUIREMENTS%"

exit /b 0

:EnsurePython
where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    python --version
    exit /b 0
)

where py >nul 2>nul
if not errorlevel 1 (
    py -3.11 --version >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_CMD=py -3.11"
        py -3.11 --version
        exit /b 0
    )
)

if exist "%PYTHON311_EXE%" (
    set "PYTHON_CMD=%PYTHON311_EXE%"
    "%PYTHON311_EXE%" --version
    exit /b 0
)

echo ERROR: Python was not found. Install Python 3.11 or newer and rerun setup.
exit /b 1
