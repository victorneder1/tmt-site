@echo off
cd /d "%~dp0"

echo ============================================
echo  Updating Excel data...
echo ============================================
python update_excel.py
if %errorlevel% neq 0 (
    echo ERROR: Excel update failed.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  Uploading to server...
echo ============================================
python upload_to_server.py
if %errorlevel% neq 0 (
    echo ERROR: Upload failed.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  All done! Data updated and uploaded.
echo ============================================
