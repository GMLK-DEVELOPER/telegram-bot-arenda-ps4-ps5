@echo off
title PlayStation Rental Bot (Go)
echo PlayStation Rental Bot - Go Edition
echo ================================================================
echo.

:: Check for Go
go version >nul 2>&1
if errorlevel 1 (
    echo Go not found! Install Go 1.21+ and add to PATH
    echo Download from https://go.dev/dl/
    pause
    exit /b 1
)

echo Go found:
go version

:: Check .env
if not exist ".env" (
    echo.
    echo .env not found - creating from example...
    copy .env.example .env
    echo Please edit .env and set your TELEGRAM_BOT_TOKEN and ADMIN_TELEGRAM_ID
    notepad .env
    echo.
    echo After editing .env, run start.bat again
    pause
    exit /b 1
)

:: Build
echo.
echo Building...
go build -o ps4-rental.exe ./cmd/
if errorlevel 1 (
    echo Build failed
    pause
    exit /b 1
)
echo Build successful

:: Run
echo.
echo Starting PlayStation Rental Bot...
echo ================================================================
echo Web panel: http://localhost:5000
echo Login: admin / Password: admin123
echo ================================================================
echo.
ps4-rental.exe

echo.
echo Bot stopped
pause
