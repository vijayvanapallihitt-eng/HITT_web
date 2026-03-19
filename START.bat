@echo off
title Broker Pipeline - Starting...
echo.
echo  ============================================
echo   Broker Pipeline
echo  ============================================
echo.
echo  Starting the application...
echo  This may take a few minutes the first time.
echo.

docker compose up -d --build

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  [!] Something went wrong.
    echo      Make sure Docker Desktop is running.
    echo.
    pause
    exit /b 1
)

echo.
echo  ============================================
echo.
echo   Ready!  Open your browser and go to:
echo.
echo       http://localhost:8000
echo.
echo  ============================================
echo.
echo  To stop the application, double-click STOP.bat
echo.
pause
