#!/usr/bin/env python3
"""
Скрипт инициализации администратора при первом запуске
"""

import os
import json
from datetime import datetime

def init_admin():
    """Создает администратора по умолчанию если его нет"""
    
    data_dir = 'data'
    admins_file = os.path.join(data_dir, 'admins.json')
    
    # Создаем папку data если её нет
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"📁 Создана папка {data_dir}")
    
    # Проверяем существование файла админов
    if os.path.exists(admins_file):
        with open(admins_file, 'r', encoding='utf-8') as f:
            try:
                admins = json.load(f)
                if admins:  # Если есть админы, ничего не делаем
                    print("👤 Администраторы уже существуют")
                    return
            except json.JSONDecodeError:
                print("⚠️ Файл администраторов поврежден, создаем заново")
    
    # Создаем администратора по умолчанию
    default_admin = {
        "admin": {
            "username": "admin", 
            "password": "admin123",
            "role": "admin",
            "created_at": datetime.now().isoformat(),
            "created_by": "system"
        }
    }
    
    with open(admins_file, 'w', encoding='utf-8') as f:
        json.dump(default_admin, f, ensure_ascii=False, indent=2)
    
    print("👤 Создан администратор по умолчанию:")
    print("   Логин: admin")
    print("   Пароль: admin123")
    print("⚠️ ОБЯЗАТЕЛЬНО смените пароль после первого входа!")

def init_data_files():
    """Создает пустые JSON файлы если их нет"""
    
    data_dir = 'data'
    files_to_init = [
        'consoles.json',
        'users.json', 
        'rentals.json',
        'rental_requests.json',
        'admin_settings.json'
    ]
    
    for filename in files_to_init:
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            print(f"📄 Создан файл {filename}")

def init_passport_dir():
    """Создает папку для документов"""
    passport_dir = 'passport'
    if not os.path.exists(passport_dir):
        os.makedirs(passport_dir)
        print(f"📁 Создана папка {passport_dir}")

if __name__ == "__main__":
    print("🚀 Инициализация проекта...")
    
    # Инициализация
    init_data_files()
    init_passport_dir() 
    init_admin()
    
    print("✅ Инициализация завершена!")
    print("🌐 Запустите проект: python run.py")
    print("🌍 Веб-панель: http://0.0.0.0:5000")