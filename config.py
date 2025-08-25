import os
import secrets
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8075876142:AAHDux8b_HScd73Vq_pHtwFCR4KDlBauPP4')

# Admin Configuration
ADMIN_TELEGRAM_ID = os.getenv('ADMIN_TELEGRAM_ID', '762139684')

# Flask Configuration
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')

# Генерируем случайный ключ если используется дефолтный
if SECRET_KEY == 'your-secret-key-here':
    SECRET_KEY = secrets.token_hex(32)
    print("⚠️ Используется автоматически сгенерированный SECRET_KEY. Установите свой в переменных окружения.")

# Database Configuration
DATABASE_CONFIG = {
    'consoles_file': 'data/consoles.json',
    'users_file': 'data/users.json',
    'rentals_file': 'data/rentals.json',
    'admins_file': 'data/admins.json'
}

