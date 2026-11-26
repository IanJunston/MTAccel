@echo off
chcp 936 >nul
title 多线程加速下载器
cd /d "%~dp0"

echo.
echo   ==========================================
echo      多线程加速下载器
echo   ==========================================
echo.

set /p URL=  粘贴下载链接后回车：
if "%URL%"=="" (
  echo.
  echo   没输入链接，退出。
  echo.
  pause
  exit /b
)

echo.
set /p NAME=  保存文件名（直接回车则自动推断）：
echo.

if "%NAME%"=="" (
  python app\cli.py "%URL%"
) else (
  python app\cli.py "%URL%" -o "%NAME%"
)

echo.
pause
