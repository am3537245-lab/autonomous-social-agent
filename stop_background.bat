@echo off
chcp 65001 >nul
echo ========================================================
echo جاري إيقاف الموجّه الآلي الذي يعمل في الخلفية...
echo ========================================================

for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000" ^| findstr "LISTENING"') do (
    echo تم العثور على العملية بالمعرف PID: %%a
    taskkill /F /PID %%a >nul 2>&1
    echo تم إيقاف الخدمة بنجاح!
    goto :done
)

echo لم يتم العثور على خدمة نشطة على المنفذ 8000.

:done
echo.
pause
