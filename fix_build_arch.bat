@echo off
echo [INFO] Removing existing build directory to reset architecture...
if exist build (
    rmdir /s /q build
)
mkdir build
cd build

echo [INFO] Configuring CMake for x64 architecture...
cmake -G "Visual Studio 17 2022" -A x64 ..

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] CMake configuration failed.
    pause
    exit /b %ERRORLEVEL%
)

echo [INFO] Configuration complete. You can now build the solution in Visual Studio.
echo [INFO] Note: Ensure "Debug | x64" is selected in Visual Studio.
pause
