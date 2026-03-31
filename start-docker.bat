@echo off
chcp 437 >nul
echo ============================================
echo  PS4 Rental - Docker Startup
echo ============================================
echo.

docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

echo [1/2] Building and starting containers...
docker-compose up --build -d

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to start. Check the error above.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  SUCCESS! Application is running.
echo ============================================
echo  URL:      http://localhost:8080/admin
echo  Login:    admin
echo  Password: admin123
echo ============================================
echo.
echo  To stop:  docker-compose down
echo  Logs:     docker-compose logs -f app
echo ============================================
pause
