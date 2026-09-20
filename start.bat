@echo off
rem Bat server SMC Monitor (bam dup vao file nay). Cua so nay phai giu mo — dong cua so = tat server.
cd /d "%~dp0"
echo ============================================
echo   SMC Monitor - dang khoi dong...
echo   Mo trinh duyet: http://localhost:8000
echo   Tat server: dong cua so nay hoac bam Ctrl+C
echo ============================================
echo.
start "" http://localhost:8000
"%~dp0venv\Scripts\python.exe" -u main.py
echo.
echo Server da dung. Bam phim bat ky de dong.
pause >nul
