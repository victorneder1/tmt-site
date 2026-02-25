@echo off
echo Starting Global AIM...
cd /d "%~dp0"
start "" http://localhost:8080
python app.py
pause
