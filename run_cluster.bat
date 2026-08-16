@echo off
echo Starting Coordinator Microservice...
start "Coordinator" cmd /c "venv\Scripts\python.exe coordinator.py"
timeout /t 2 /nobreak > NUL

echo Starting 3 Writer instances...
start "Writer 1" cmd /c "venv\Scripts\python.exe writer.py"
start "Writer 2" cmd /c "venv\Scripts\python.exe writer.py"
start "Writer 3" cmd /c "venv\Scripts\python.exe writer.py"

echo Cluster started! You can close this window.
pause
