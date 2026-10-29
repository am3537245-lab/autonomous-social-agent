@echo off
chcp 65001 >nul
echo ========================================================
echo تفعيل التشغيل التلقائي مع إقلاع ويندوز (Auto-Start)
echo ========================================================

set SCRIPT_DIR=%~dp0
set TARGET=%SCRIPT_DIR%start_background.vbs
set SHORTCUT=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\AutonomousAgent.vbs

copy /y "%TARGET%" "%SHORTCUT%" >nul

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✓ تم بنجاح ضبط النظام ليبدأ العمل تلقائياً في الخلفية كلما قمت بفتح اللابتوب!
    echo مسار ملف الإقلاع: %SHORTCUT%
) else (
    echo.
    echo ✗ حدث خطأ أثناء إضافة الملف إلى مجلد بدء التشغيل.
)

echo.
pause
