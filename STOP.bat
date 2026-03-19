@echo off
title Broker Pipeline - Stopping...
echo.
echo  Stopping the application...
echo.

docker compose down

echo.
echo  ============================================
echo.
echo   Application stopped.
echo   Your data is saved and will be there
echo   next time you start.
echo.
echo  ============================================
echo.
pause
