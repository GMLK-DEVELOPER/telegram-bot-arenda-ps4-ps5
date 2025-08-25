import threading
import time
import json
import os
from datetime import datetime, timedelta
from bot import load_json_file, save_json_file, notify_admin, notify_user_about_rental_end, safe_send_message

DATA_DIR = 'data'
CONSOLES_FILE = os.path.join(DATA_DIR, 'consoles.json')
USERS_FILE = os.path.join(DATA_DIR, 'users.json')
RENTALS_FILE = os.path.join(DATA_DIR, 'rentals.json')
ADMIN_SETTINGS_FILE = os.path.join(DATA_DIR, 'admin_settings.json')

class RentalScheduler:
    def __init__(self):
        self.running = False
        self.thread = None
        
    def start(self):
        """Запуск планировщика"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.thread.start()
            print("🕒 Планировщик аренды запущен")
    
    def stop(self):
        """Остановка планировщика"""
        self.running = False
        if self.thread:
            self.thread.join()
            print("🕒 Планировщик аренды остановлен")
    
    def _run_scheduler(self):
        """Основной цикл планировщика"""
        while self.running:
            try:
                self._check_expired_rentals()
                self._send_rental_reminders()
            except Exception as e:
                print(f"Ошибка в планировщике аренды: {e}")
            
            # Проверяем с интервалом из настроек (по умолчанию каждые 5 минут)
            settings = load_json_file(ADMIN_SETTINGS_FILE)
            check_interval = settings.get('notification_frequency', 5) * 60  # конвертируем минуты в секунды
            time.sleep(check_interval)
    
    def _check_expired_rentals(self):
        """Проверка и завершение просроченных аренд"""
        settings = load_json_file(ADMIN_SETTINGS_FILE)
        max_rental_hours = settings.get('max_rental_hours', 24)  # Максимум 24 часа по умолчанию
        
        rentals = load_json_file(RENTALS_FILE)
        consoles = load_json_file(CONSOLES_FILE)
        users = load_json_file(USERS_FILE)
        
        current_time = datetime.now()
        expired_rentals = []
        
        for rental_id, rental in rentals.items():
            if rental['status'] == 'active':
                start_time = datetime.fromisoformat(rental['start_time'])
                duration = current_time - start_time
                hours = duration.total_seconds() / 3600
                
                # Если аренда превысила максимальное время
                if hours >= max_rental_hours:
                    expired_rentals.append((rental_id, rental, hours))
        
        # Завершаем просроченные аренды
        for rental_id, rental, hours in expired_rentals:
            try:
                console = consoles.get(rental['console_id'], {})
                user = users.get(rental['user_id'], {})
                
                # Рассчитываем стоимость
                total_hours = max(1, int(hours))
                total_cost = total_hours * console.get('rental_price', 0)
                
                # Завершаем аренду
                rental['end_time'] = current_time.isoformat()
                rental['status'] = 'completed'
                rental['total_cost'] = total_cost
                rental['ended_by'] = 'system_timeout'
                
                # Освобождаем консоль
                if rental['console_id'] in consoles:
                    consoles[rental['console_id']]['status'] = 'available'
                
                # Обновляем статистику пользователя
                if rental['user_id'] in users:
                    users[rental['user_id']]['total_spent'] = users[rental['user_id']].get('total_spent', 0) + total_cost
                
                print(f"⏰ Автоматически завершена аренда {rental_id} (превышен лимит времени)")
                
                # Уведомления
                self._notify_about_auto_end(rental['user_id'], console, total_cost, total_hours, user)
                
            except Exception as e:
                print(f"Ошибка при автоматическом завершении аренды {rental_id}: {e}")
        
        if expired_rentals:
            save_json_file(RENTALS_FILE, rentals)
            save_json_file(CONSOLES_FILE, consoles)
            save_json_file(USERS_FILE, users)
    
    def _send_rental_reminders(self):
        """Отправка напоминаний о скором завершении аренды"""
        settings = load_json_file(ADMIN_SETTINGS_FILE)
        reminder_hours = settings.get('reminder_hours', 23)  # Напоминание за час до завершения
        max_rental_hours = settings.get('max_rental_hours', 24)
        
        rentals = load_json_file(RENTALS_FILE)
        consoles = load_json_file(CONSOLES_FILE)
        users = load_json_file(USERS_FILE)
        
        current_time = datetime.now()
        
        # Проверяем настройки push-уведомлений
        push_enabled = settings.get('push_notifications_enabled', True)
        critical_enabled = settings.get('critical_notifications_enabled', True)
        
        if not push_enabled:
            return  # Уведомления отключены
        
        for rental_id, rental in rentals.items():
            if rental['status'] == 'active':
                start_time = datetime.fromisoformat(rental['start_time'])
                duration = current_time - start_time
                hours = duration.total_seconds() / 3600
                
                # Множественные напоминания
                remaining_hours = max_rental_hours - hours
                console = consoles.get(rental['console_id'], {})
                
                # За 2 часа до окончания
                if (remaining_hours <= 2 and remaining_hours > 1.5 and 
                    not rental.get('reminder_2h_sent', False)):
                    self._send_push_notification(rental, console, remaining_hours, "2_hours", rental_id)
                    rental['reminder_2h_sent'] = True
                    save_json_file(RENTALS_FILE, rentals)
                
                # За 1 час до окончания
                elif (remaining_hours <= 1 and remaining_hours > 0.5 and 
                      not rental.get('reminder_1h_sent', False)):
                    self._send_push_notification(rental, console, remaining_hours, "1_hour", rental_id)
                    rental['reminder_1h_sent'] = True
                    save_json_file(RENTALS_FILE, rentals)
                
                # За 30 минут до окончания
                elif (remaining_hours <= 0.5 and remaining_hours > 0.25 and 
                      not rental.get('reminder_30m_sent', False)):
                    self._send_push_notification(rental, console, remaining_hours, "30_minutes", rental_id)
                    rental['reminder_30m_sent'] = True
                    save_json_file(RENTALS_FILE, rentals)
                
                # За 10 минут до окончания (критическое уведомление)
                elif (remaining_hours <= 0.17 and remaining_hours > 0.08 and 
                      not rental.get('reminder_10m_sent', False) and critical_enabled):
                    self._send_push_notification(rental, console, remaining_hours, "10_minutes", rental_id)
                    rental['reminder_10m_sent'] = True
                    save_json_file(RENTALS_FILE, rentals)
    
    def _notify_about_auto_end(self, user_id, console, total_cost, hours, user):
        """Уведомления об автоматическом завершении аренды"""
        try:
            # Уведомление пользователю
            user_message = f"⏰ **Аренда автоматически завершена**\n\n"
            user_message += f"🎮 Консоль: {console.get('name', 'Неизвестная консоль')}\n"
            user_message += f"⏰ Длительность: {hours} часов\n"
            user_message += f"💰 К оплате: {total_cost} лей\n\n"
            user_message += f"🕒 Аренда была завершена автоматически по истечении максимального времени.\n"
            user_message += f"Спасибо за использование нашего сервиса!"
            
            safe_send_message(user_id, user_message)
            
            # Уведомление администратору
            admin_message = f"⏰ **Аренда автоматически завершена**\n\n"
            admin_message += f"👤 {user.get('full_name', user.get('first_name', 'Неизвестный'))}\n"
            admin_message += f"📱 {user.get('phone_number', 'Не указан')}\n"
            admin_message += f"🎮 {console.get('name', 'Неизвестная консоль')}\n"
            admin_message += f"⏰ {hours} часов\n"
            admin_message += f"💰 {total_cost} лей\n"
            admin_message += f"🤖 Завершено системой (превышен лимит времени)"
            
            notify_admin(admin_message)
            
        except Exception as e:
            print(f"Ошибка отправки уведомлений об автозавершении: {e}")
    
    def _send_push_notification(self, rental, console, remaining_hours, notification_type, rental_id):
        """Отправка push-уведомления пользователю"""
        try:
            user_id = rental['user_id']
            console_name = console.get('name', 'Неизвестная консоль')
            
            # Определяем тип уведомления и соответствующее сообщение
            notification_configs = {
                "2_hours": {
                    "emoji": "⏰",
                    "title": "Напоминание об аренде",
                    "urgency": "info",
                    "time_text": "2 часа"
                },
                "1_hour": {
                    "emoji": "🕐",
                    "title": "Скоро завершение аренды",
                    "urgency": "warning", 
                    "time_text": "1 час"
                },
                "30_minutes": {
                    "emoji": "⚠️",
                    "title": "ВНИМАНИЕ! Скоро завершение",
                    "urgency": "high",
                    "time_text": "30 минут"
                },
                "10_minutes": {
                    "emoji": "🚨",
                    "title": "КРИТИЧЕСКОЕ! Последние минуты",
                    "urgency": "critical",
                    "time_text": "10 минут"
                }
            }
            
            config = notification_configs.get(notification_type, notification_configs["1_hour"])
            
            # Формируем сообщение в зависимости от срочности
            if config["urgency"] == "critical":
                message = f"{config['emoji']} **{config['title']}** {config['emoji']}\n\n"
                message += f"🎮 Консоль: **{console_name}**\n"
                message += f"⏳ До завершения: **{config['time_text']}**\n\n"
                message += f"🚨 **СРОЧНО ЗАВЕРШИТЕ АРЕНДУ!**\n"
                message += f"💰 Текущая стоимость: {self._calculate_current_cost(rental, console)} лей\n\n"
            elif config["urgency"] == "high":
                message = f"{config['emoji']} **{config['title']}**\n\n"
                message += f"🎮 Консоль: {console_name}\n"
                message += f"⏳ До завершения: **{config['time_text']}**\n\n"
                message += f"⚡ Рекомендуем завершить аренду заранее\n"
                message += f"💰 Текущая стоимость: {self._calculate_current_cost(rental, console)} лей\n\n"
            else:
                message = f"{config['emoji']} **{config['title']}**\n\n"
                message += f"🎮 Консоль: {console_name}\n"
                message += f"⏳ До завершения: {config['time_text']}\n\n"
            
            # Добавляем инструкции по завершению
            message += f"💡 **Способы завершения:**\n"
            message += f"• Команда: `/end {rental_id}`\n"
            message += f"• Кнопка в \"📊 Мой кабинет\"\n"
            message += f"• Веб-панель администратора\n\n"
            message += f"📞 Помощь: напишите администратору"
            
            from bot import bot
            
            # Отправляем с повышенным приоритетом для критических уведомлений
            if config["urgency"] == "critical":
                # Отправляем два сообщения для критических случаев
                safe_send_message(user_id, "🚨🚨🚨 КРИТИЧЕСКОЕ УВЕДОМЛЕНИЕ 🚨🚨🚨")
                safe_send_message(user_id, message)
            else:
                safe_send_message(user_id, message)
            
            print(f"🔔 Push-уведомление ({notification_type}) отправлено для аренды {rental_id}")
            
        except Exception as e:
            print(f"Ошибка отправки push-уведомления: {e}")
    
    def _calculate_current_cost(self, rental, console):
        """Рассчитать текущую стоимость аренды"""
        try:
            start_time = datetime.fromisoformat(rental['start_time'])
            current_time = datetime.now()
            duration = current_time - start_time
            hours = max(1, int(duration.total_seconds() / 3600))
            return hours * console.get('rental_price', 0)
        except:
            return 0

    def _send_reminder_to_user(self, user_id, console, remaining_hours, rental_id):
        """Отправка напоминания пользователю (устаревшая функция)"""
        try:
            reminder_message = f"⏰ **Напоминание об аренде**\n\n"
            reminder_message += f"🎮 Консоль: {console.get('name', 'Неизвестная консоль')}\n"
            reminder_message += f"⏳ До автоматического завершения: {remaining_hours:.1f} часов\n\n"
            reminder_message += f"💡 Вы можете завершить аренду досрочно через команду:\n"
            reminder_message += f"`/end {rental_id}`\n\n"
            reminder_message += f"Или через кнопку в разделе \"📊 Мой кабинет\""
            
            from bot import bot
            safe_send_message(user_id, reminder_message)
            
        except Exception as e:
            print(f"Ошибка отправки напоминания: {e}")

# Глобальный экземпляр планировщика
scheduler = RentalScheduler()

def start_rental_scheduler():
    """Запуск планировщика аренды"""
    scheduler.start()

def stop_rental_scheduler():
    """Остановка планировщика аренды"""
    scheduler.stop()