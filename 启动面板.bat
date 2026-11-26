@echo off
chcp 936 >nul
title 多线程加速下载器 · 面板
cd /d "%~dp0"

echo.
echo   多线程加速下载器 · 正在启动面板...
echo   浏览器会自动打开，关闭本窗口即停止服务。
echo.

python app\server.py %*
if errorlevel 1 (
  echo.
  echo   [启动失败] 请先安装依赖：pip install -r requirements.txt
  echo.
  pause
)
