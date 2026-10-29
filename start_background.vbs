' Launches the Autonomous Social Media Daemon completely hidden in the background
Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

' Get current directory
currentDir = fso.GetParentFolderName(WScript.ScriptFullName)

' Python executable path
pythonExe = "C:\Users\MC\AppData\Local\Programs\Python\Python311\python.exe"

' Launch command silently (window style 0 = hidden, False = don't wait for completion)
cmd = """" & pythonExe & """ """ & currentDir & "\main.py"" --port 8000"
WshShell.Run cmd, 0, False

WScript.Echo "تم تشغيل الموجّه الآلي بنجاح في خلفية النظام (صامت 24/7)!" & vbCrLf & "يمكنك فتح لوحة التحكم من هاتفك أو المتصفح: http://localhost:8000"
