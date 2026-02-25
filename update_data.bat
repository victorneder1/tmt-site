@echo off
echo Updating Excel data...
cd /d "%~dp0"
python update_excel.py
echo Done.
