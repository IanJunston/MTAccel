@echo off
chcp 936 >nul
title 安装依赖 · 多线程加速下载器
cd /d "%~dp0"

echo.
echo   ==========================================
echo      多线程加速下载器 · 依赖安装
echo   ==========================================
echo.

echo   [1/2] 检查 Python 环境...
python --version 2>nul
if errorlevel 1 (
  echo.
  echo   [错误] 没有找到 Python。
  echo.
  echo   请先安装 Python 3.11 或更高版本：
  echo       https://www.python.org/downloads/
  echo   安装第一步一定要勾选  "Add python.exe to PATH"
  echo.
  pause
  exit /b
)

echo.
echo   [2/2] 安装依赖（清华镜像，通常十几秒）...
echo.
python -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple >nul 2>&1
python -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
  echo.
  echo   镜像源失败，改用官方源重试...
  python -m pip install -r requirements.txt
)

echo.
echo   ==========================================
echo      安装完成，现在可以双击：
echo        启动面板.bat   （网页版，推荐）
echo        下载.bat       （命令行版）
echo   ==========================================
echo.
pause
