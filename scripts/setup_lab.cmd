@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "REPO_ROOT=%~dp0.."
set "VENV_DIR=%REPO_ROOT%\.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "AWS_CMD=%PYTHON_EXE% -m awscli"
set "REQUIREMENTS=%REPO_ROOT%\requirements.txt"
set "ENV_FILE=%REPO_ROOT%\.env"
set "TFVARS=%REPO_ROOT%\terraform\terraform.tfvars"
set "PYTHON311_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
set "TERRAFORM_EXE=terraform.exe"
set "LOCAL_TERRAFORM_DIR=%REPO_ROOT%\.tools\terraform"
set "LOCAL_TERRAFORM=%LOCAL_TERRAFORM_DIR%\terraform.exe"
set "WINGET_TERRAFORM=%LOCALAPPDATA%\Microsoft\WinGet\Packages\Hashicorp.Terraform_Microsoft.Winget.Source_8wekyb3d8bbwe\terraform.exe"

echo Iceberg AWS Lab Setup
echo.

if not exist "%ENV_FILE%" (
    echo ERROR: .env file not found.
    echo Copy user.env.example to .env and fill in AWS values first.
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%ENV_FILE%") do (
    set "KEY=%%A"
    set "VALUE=%%B"
    if not "!KEY!"=="" if not "!KEY:~0,1!"=="#" set "!KEY!=!VALUE!"
)

if "%AWS_ACCESS_KEY_ID%"=="" (
    echo ERROR: AWS_ACCESS_KEY_ID is missing in .env.
    exit /b 1
)
if "%AWS_SECRET_ACCESS_KEY%"=="" (
    echo ERROR: AWS_SECRET_ACCESS_KEY is missing in .env.
    exit /b 1
)
if "%AWS_DEFAULT_REGION%"=="" set "AWS_DEFAULT_REGION=us-east-1"
if "%TF_VAR_bucket_suffix%"=="" (
    echo ERROR: TF_VAR_bucket_suffix is missing in .env.
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
echo Installing project tools...
"%PYTHON_EXE%" -m pip install --upgrade pip
if errorlevel 1 exit /b 1
"%PYTHON_EXE%" -m pip install -r "%REQUIREMENTS%"
if errorlevel 1 exit /b 1

echo.
echo Configuring AWS CLI from .env...
%AWS_CMD% configure set aws_access_key_id "%AWS_ACCESS_KEY_ID%"
if errorlevel 1 exit /b 1
%AWS_CMD% configure set aws_secret_access_key "%AWS_SECRET_ACCESS_KEY%"
if errorlevel 1 exit /b 1
%AWS_CMD% configure set region "%AWS_DEFAULT_REGION%"
if errorlevel 1 exit /b 1
%AWS_CMD% configure set output json
if errorlevel 1 exit /b 1

echo.
echo Validating AWS identity...
%AWS_CMD% sts get-caller-identity
if errorlevel 1 (
    echo ERROR: AWS identity validation failed. Check the keys in .env.
    exit /b 1
)

echo.
echo Checking Terraform...
call :EnsureTerraform || exit /b 1
"%TERRAFORM_EXE%" version
if errorlevel 1 exit /b 1

echo.
echo Writing Terraform variables...
echo bucket_suffix = "%TF_VAR_bucket_suffix%" > "%TFVARS%"
if errorlevel 1 exit /b 1

echo.
echo Initializing Terraform...
call "%REPO_ROOT%\scripts\terraform.cmd" init
if errorlevel 1 exit /b 1

echo.
echo Validating Terraform...
call "%REPO_ROOT%\scripts\terraform.cmd" validate
if errorlevel 1 exit /b 1

echo.
echo Setup complete.
echo.
echo Create infra:
echo scripts\terraform.cmd plan
echo scripts\terraform.cmd apply
echo.
echo Destroy infra:
echo scripts\terraform.cmd destroy

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

echo Python was not found. Installing Python 3.11 with winget...
where winget >nul 2>nul
if errorlevel 1 (
    echo ERROR: winget is not available. Install Python 3.11 manually.
    exit /b 1
)

winget install --id Python.Python.3.11 --exact --accept-package-agreements --accept-source-agreements
if errorlevel 1 exit /b 1

where py >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.11"
    py -3.11 --version
    exit /b 0
)

if exist "%PYTHON311_EXE%" (
    set "PYTHON_CMD=%PYTHON311_EXE%"
    "%PYTHON311_EXE%" --version
    exit /b 0
)

where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    python --version
    exit /b 0
)

echo ERROR: Python installed, but this terminal cannot find it. Open a new CMD terminal and rerun setup.
exit /b 1

:EnsureTerraform
if exist "%LOCAL_TERRAFORM%" (
    set "TERRAFORM_EXE=%LOCAL_TERRAFORM%"
    exit /b 0
)

where terraform.exe >nul 2>nul
if not errorlevel 1 (
    set "TERRAFORM_EXE=terraform.exe"
    exit /b 0
)

if exist "%WINGET_TERRAFORM%" (
    call :CacheTerraform "%WINGET_TERRAFORM%"
    set "TERRAFORM_EXE=%LOCAL_TERRAFORM%"
    exit /b 0
)

echo Terraform was not found. Installing Terraform with winget...
where winget >nul 2>nul
if errorlevel 1 (
    echo ERROR: winget is not available. Install Terraform manually.
    exit /b 1
)

winget install --id Hashicorp.Terraform --exact --accept-package-agreements --accept-source-agreements
if errorlevel 1 (
    winget list --id Hashicorp.Terraform --exact >nul 2>nul
    if errorlevel 1 exit /b 1
)

where terraform.exe >nul 2>nul
if not errorlevel 1 (
    set "TERRAFORM_EXE=terraform.exe"
    exit /b 0
)

if exist "%WINGET_TERRAFORM%" (
    call :CacheTerraform "%WINGET_TERRAFORM%"
    set "TERRAFORM_EXE=%LOCAL_TERRAFORM%"
    exit /b 0
)

echo ERROR: Terraform installed, but setup could not find terraform.exe. Open a new CMD terminal and rerun setup.
exit /b 1

:CacheTerraform
if not exist "%LOCAL_TERRAFORM_DIR%" mkdir "%LOCAL_TERRAFORM_DIR%"
copy /Y "%~1" "%LOCAL_TERRAFORM%" >nul
if errorlevel 1 exit /b 1
exit /b 0
