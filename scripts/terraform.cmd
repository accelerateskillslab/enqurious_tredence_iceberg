@echo off
setlocal

set "REPO_ROOT=%~dp0.."
set "TERRAFORM_EXE=terraform.exe"
set "LOCAL_TERRAFORM=%REPO_ROOT%\.tools\terraform\terraform.exe"
set "WINGET_TERRAFORM=%LOCALAPPDATA%\Microsoft\WinGet\Packages\Hashicorp.Terraform_Microsoft.Winget.Source_8wekyb3d8bbwe\terraform.exe"

if exist "%LOCAL_TERRAFORM%" (
    set "TERRAFORM_EXE=%LOCAL_TERRAFORM%"
) else (
    where terraform.exe >nul 2>nul
    if errorlevel 1 (
        if exist "%WINGET_TERRAFORM%" (
            set "TERRAFORM_EXE=%WINGET_TERRAFORM%"
        ) else (
            echo ERROR: Terraform was not found. Run scripts\setup_lab.cmd first.
            exit /b 1
        )
    )
)

"%TERRAFORM_EXE%" -chdir="%REPO_ROOT%\terraform" %*

endlocal
