@echo off
title PlayStation Rental Bot
echo 🎮 PlayStation Rental Bot - Автоматическая настройка и запуск
echo ================================================================
echo.

:: Проверяем наличие Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python не найден! Установите Python 3.7+ и добавьте в PATH
    echo 💡 Скачайте с https://python.org
    pause
    exit /b 1
)

echo ✅ Python найден
python --version

:: Проверяем наличие виртуального окружения
if not exist "venv" (
    echo.
    echo 📦 Виртуальное окружение не найдено. Создаю venv...
    python -m venv venv
    if errorlevel 1 (
        echo ❌ Ошибка создания виртуального окружения
        pause
        exit /b 1
    )
    echo ✅ Виртуальное окружение создано
) else (
    echo ✅ Виртуальное окружение найдено
)

:: Активируем виртуальное окружение
echo.
echo 🔄 Активация виртуального окружения...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ❌ Ошибка активации виртуального окружения
    pause
    exit /b 1
)

:: Проверяем и устанавливаем зависимости
echo.
echo 📋 Проверка зависимостей...
pip list | findstr "Flask" >nul 2>&1
if errorlevel 1 (
    echo 📥 Установка зависимостей из requirements.txt...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ❌ Ошибка установки зависимостей
        pause
        exit /b 1
    )
    echo ✅ Зависимости установлены
) else (
    echo ✅ Зависимости уже установлены
)

:: Запускаем проект
echo.
echo Запуск PlayStation Rental Bot...
echo ================================================================
echo Веб-панель будет доступна: http://localhost:5000
echo Логин: admin / Пароль: admin123
echo Найдите своего бота в Telegram
echo ================================================================
echo.
python run.py

echo.
echo Проект остановлен
pause