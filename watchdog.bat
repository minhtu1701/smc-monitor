@echo off
rem Canh server SMC Monitor - tu bat lai neu server chet. Giu cua so nay mo.
cd /d "%~dp0"
echo ============================================
echo   SMC Monitor - WATCHDOG
echo   Kiem tra server moi 60 giay, chet thi bat lai
echo   Dong cua so nay de tat watchdog
echo ============================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0watchdog.ps1"
pause
