import asyncio
import aiohttp
import aiofiles
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import wraps
import time

from performance_optimizer import (
    db_manager, file_handler, data_processor, memory_optimizer,
    async_cached, memory_optimized
)

logger = logging.getLogger(__name__)

class AsyncBotHandler:
    """Асинхронный обработчик бота с высокой производительностью"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=16)
        
        # Очереди для асинхронной обработки
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.callback_queue = asyncio.Queue(maxsize=500)
        self.file_queue = asyncio.Queue(maxsize=100)
        
        # Статистика производительности
        self.stats = {
            'messages_processed': 0,
            'files_processed': 0,
            'start_time': time.time(),
            'errors': 0
        }
        
        logger.info("Асинхронный обработчик бота инициализирован")
    
    async def start(self):
        """Запуск асинхронного обработчика"""
        try:
            # Создание HTTP сессии с оптимизацией
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
            
            # Запуск воркеров для обработки очередей
            tasks = [
                asyncio.create_task(self._message_worker()),
                asyncio.create_task(self._callback_worker()),
                asyncio.create_task(self._file_worker()),
                asyncio.create_task(self._stats_worker())
            ]
            
            logger.info("Асинхронный обработчик запущен")
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Ошибка запуска асинхронного обработчика: {e}")
            raise
    
    async def stop(self):
        """Остановка обработчика"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=True)
        logger.info("Асинхронный обработчик остановлен")
    
    async def _message_worker(self):
        """Воркер для обработки сообщений"""
        while True:
            try:
                # Получаем сообщение из очереди
                message_data = await self.message_queue.get()
                
                # Обрабатываем сообщение асинхронно
                await self._process_message_async(message_data)
                
                # Отмечаем задачу как выполненную
                self.message_queue.task_done()
                self.stats['messages_processed'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в message_worker: {e}")
                self.stats['errors'] += 1
    
    async def _callback_worker(self):
        """Воркер для обработки callback запросов"""
        while True:
            try:
                callback_data = await self.callback_queue.get()
                await self._process_callback_async(callback_data)
                self.callback_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в callback_worker: {e}")
                self.stats['errors'] += 1
    
    async def _file_worker(self):
        """Воркер для обработки файлов"""
        while True:
            try:
                file_data = await self.file_queue.get()
                await self._process_file_async(file_data)
                self.file_queue.task_done()
                self.stats['files_processed'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в file_worker: {e}")
                self.stats['errors'] += 1
    
    async def _stats_worker(self):
        """Воркер для сбора статистики"""
        while True:
            try:
                await asyncio.sleep(60)  # Каждую минуту
                await self._collect_stats()
                memory_optimizer.optimize_memory()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в stats_worker: {e}")
    
    async def add_message_to_queue(self, message_data: Dict) -> bool:
        """Добавление сообщения в очередь обработки"""
        try:
            await self.message_queue.put(message_data)
            return True
        except asyncio.QueueFull:
            logger.warning("Очередь сообщений переполнена")
            return False
    
    async def add_callback_to_queue(self, callback_data: Dict) -> bool:
        """Добавление callback в очередь обработки"""
        try:
            await self.callback_queue.put(callback_data)
            return True
        except asyncio.QueueFull:
            logger.warning("Очередь callback переполнена")
            return False
    
    async def add_file_to_queue(self, file_data: Dict) -> bool:
        """Добавление файла в очередь обработки"""
        try:
            await self.file_queue.put(file_data)
            return True
        except asyncio.QueueFull:
            logger.warning("Очередь файлов переполнена")
            return False
    
    @async_cached(ttl=300)
    async def _process_message_async(self, message_data: Dict):
        """Асинхронная обработка сообщения"""
        try:
            message_type = message_data.get('type', 'text')
            user_id = str(message_data.get('user_id', ''))
            
            # Загрузка данных пользователя асинхронно
            users = await db_manager.load_data('users.json')
            
            # Проверка бана
            if users.get(user_id, {}).get('is_banned', False):
                await self._send_message_async(user_id, "❌ Вы заблокированы!")
                return
            
            # Обработка в зависимости от типа
            if message_type == 'start':
                await self._handle_start_async(message_data, users)
            elif message_type == 'consoles':
                await self._handle_consoles_async(message_data)
            elif message_type == 'profile':
                await self._handle_profile_async(message_data, users)
            elif message_type == 'rental_request':
                await self._handle_rental_request_async(message_data)
                
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
    
    async def _handle_start_async(self, message_data: Dict, users: Dict):
        """Асинхронная обработка команды start"""
        user_id = str(message_data.get('user_id', ''))
        user_info = message_data.get('user_info', {})
        
        # Добавление нового пользователя если нужно
        if user_id not in users:
            users[user_id] = {
                'id': user_id,
                'username': user_info.get('username'),
                'first_name': user_info.get('first_name'),
                'is_banned': False,
                'rentals': [],
                'joined_at': datetime.now().isoformat()
            }
            
            # Сохранение асинхронно
            await db_manager.save_data('users.json', users)
        
        # Отправка приветствия
        welcome_text = "🎮 Добро пожаловать в систему аренды PlayStation!\n\nВыберите действие:"
        await self._send_message_async(user_id, welcome_text)
    
    @async_cached(ttl=60)
    async def _handle_consoles_async(self, message_data: Dict):
        """Асинхронная обработка запроса консолей"""
        user_id = str(message_data.get('user_id', ''))
        
        # Загрузка консолей асинхронно
        consoles = await db_manager.load_data('consoles.json')
        
        if not consoles:
            await self._send_message_async(user_id, "📭 Консоли пока недоступны")
            return
        
        # Формирование ответа с использованием многопроцессорности
        console_list = list(consoles.values())
        formatted_consoles = data_processor.process_data_parallel(
            console_list, 
            self._format_console_info
        )
        
        response = "🎮 Доступные консоли:\n\n" + "\n\n".join(formatted_consoles)
        await self._send_message_async(user_id, response)
    
    @staticmethod
    def _format_console_info(console: Dict) -> str:
        """Форматирование информации о консоли"""
        status_emoji = "✅" if console.get('status') == 'available' else "❌"
        games = console.get('games', [])
        games_text = ", ".join(games[:3]) + ("..." if len(games) > 3 else "")
        
        return (f"{status_emoji} {console.get('name', 'Неизвестная')} "
                f"({console.get('model', 'Неизвестная модель')})\n"
                f"💰 Аренда: {console.get('rental_price', 0)} лей/час\n"
                f"🎯 Игры: {games_text}\n"
                f"🆔 ID: {console.get('id', 'Неизвестный')}")
    
    async def _handle_profile_async(self, message_data: Dict, users: Dict):
        """Асинхронная обработка профиля пользователя"""
        user_id = str(message_data.get('user_id', ''))
        
        if user_id not in users:
            await self._send_message_async(user_id, "❌ Пользователь не найден")
            return
        
        # Параллельная загрузка данных аренд
        rentals = await db_manager.load_data('rentals.json')
        
        user = users[user_id]
        user_rentals = [r for r in rentals.values() if r.get('user_id') == user_id]
        active_rentals = [r for r in user_rentals if r.get('status') == 'active']
        
        response = f"👤 Ваш профиль:\n\n"
        response += f"🆔 ID: {user_id}\n"
        response += f"👤 Имя: {user.get('first_name', 'Не указано')}\n"
        response += f"📅 Регистрация: {user.get('joined_at', '')[:10]}\n"
        response += f"📊 Всего аренд: {len(user_rentals)}\n"
        response += f"🔄 Активных аренд: {len(active_rentals)}\n"
        
        if active_rentals:
            response += "\n🎮 Активные аренды:\n"
            for rental in active_rentals:
                response += f"• Консоль ID: {rental.get('console_id', 'Неизвестная')}\n"
                response += f"  Начало: {rental.get('start_time', '')[:16]}\n"
        
        await self._send_message_async(user_id, response)
    
    async def _process_callback_async(self, callback_data: Dict):
        """Асинхронная обработка callback запросов"""
        try:
            callback_type = callback_data.get('type', '')
            user_id = str(callback_data.get('user_id', ''))
            data = callback_data.get('data', {})
            
            if callback_type == 'rent_console':
                await self._handle_rent_console_async(user_id, data)
            elif callback_type == 'end_rental':
                await self._handle_end_rental_async(user_id, data)
            elif callback_type == 'extend_rental':
                await self._handle_extend_rental_async(user_id, data)
                
        except Exception as e:
            logger.error(f"Ошибка обработки callback: {e}")
    
    async def _process_file_async(self, file_data: Dict):
        """Асинхронная обработка файлов"""
        try:
            file_type = file_data.get('type', '')
            user_id = str(file_data.get('user_id', ''))
            file_info = file_data.get('file_info', {})
            
            if file_type == 'passport':
                await self._handle_passport_upload_async(user_id, file_info)
            elif file_type == 'console_image':
                await self._handle_console_image_upload_async(file_info)
                
        except Exception as e:
            logger.error(f"Ошибка обработки файла: {e}")
    
    async def _handle_passport_upload_async(self, user_id: str, file_info: Dict):
        """Асинхронная обработка загрузки паспорта"""
        try:
            # Скачивание файла асинхронно
            file_path = await self._download_file_async(file_info['file_id'])
            
            if file_path:
                # Проверка и оптимизация изображения
                optimization_result = await file_handler.process_file_async(file_path, 'process_image')
                
                if optimization_result:
                    # Перемещение в папку пользователя
                    user_folder = await self._create_user_folder_async(user_id)
                    final_path = os.path.join(user_folder, f"{file_info['document_type']}.jpg")
                    
                    # Атомарное перемещение файла
                    os.rename(optimization_result['optimized_path'], final_path)
                    
                    await self._send_message_async(user_id, "✅ Документ успешно загружен и обработан")
                else:
                    await self._send_message_async(user_id, "❌ Ошибка обработки документа")
            else:
                await self._send_message_async(user_id, "❌ Ошибка скачивания файла")
                
        except Exception as e:
            logger.error(f"Ошибка обработки паспорта: {e}")
    
    async def _download_file_async(self, file_id: str) -> Optional[str]:
        """Асинхронное скачивание файла"""
        try:
            # Получение информации о файле
            file_info_url = f"{self.api_url}/getFile?file_id={file_id}"
            
            async with self.session.get(file_info_url) as response:
                if response.status == 200:
                    data = await response.json()
                    file_path = data['result']['file_path']
                    
                    # Скачивание файла
                    file_url = f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}"
                    
                    async with self.session.get(file_url) as file_response:
                        if file_response.status == 200:
                            # Проверка размера файла
                            content_length = file_response.headers.get('content-length')
                            if content_length and int(content_length) > file_handler.max_file_size:
                                logger.warning(f"Файл {file_id} превышает максимальный размер")
                                return None
                            
                            # Сохранение во временный файл
                            temp_path = f"temp_{file_id}_{int(time.time())}.tmp"
                            
                            async with aiofiles.open(temp_path, 'wb') as f:
                                async for chunk in file_response.content.iter_chunked(8192):
                                    await f.write(chunk)
                            
                            return temp_path
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка скачивания файла {file_id}: {e}")
            return None
    
    async def _create_user_folder_async(self, user_id: str) -> str:
        """Асинхронное создание папки пользователя"""
        users = await db_manager.load_data('users.json')
        user = users.get(user_id, {})
        user_name = user.get('full_name', user.get('first_name', f'user_{user_id}'))
        
        # Безопасное имя папки
        safe_name = "".join(c for c in user_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        user_folder = os.path.join('passport', safe_name)
        
        os.makedirs(user_folder, exist_ok=True)
        return user_folder
    
    async def _send_message_async(self, chat_id: str, text: str, 
                                 reply_markup: Optional[Dict] = None) -> bool:
        """Асинхронная отправка сообщения"""
        try:
            url = f"{self.api_url}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'Markdown'
            }
            
            if reply_markup:
                data['reply_markup'] = json.dumps(reply_markup)
            
            async with self.session.post(url, json=data) as response:
                success = response.status == 200
                if not success:
                    error_text = await response.text()
                    logger.error(f"Ошибка отправки сообщения: {error_text}")
                
                return success
                
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения в {chat_id}: {e}")
            return False
    
    async def _collect_stats(self):
        """Сбор статистики производительности"""
        try:
            current_time = time.time()
            uptime = current_time - self.stats['start_time']
            
            stats_data = {
                'uptime_seconds': uptime,
                'messages_processed': self.stats['messages_processed'],
                'files_processed': self.stats['files_processed'],
                'errors': self.stats['errors'],
                'messages_per_second': self.stats['messages_processed'] / uptime if uptime > 0 else 0,
                'queue_sizes': {
                    'messages': self.message_queue.qsize(),
                    'callbacks': self.callback_queue.qsize(),
                    'files': self.file_queue.qsize()
                },
                'timestamp': datetime.now().isoformat()
            }
            
            # Сохранение статистики
            await db_manager.save_data('performance_stats.json', stats_data)
            
            logger.info(f"Статистика: {self.stats['messages_processed']} сообщений, "
                       f"{self.stats['files_processed']} файлов, "
                       f"{stats_data['messages_per_second']:.2f} msg/sec")
                       
        except Exception as e:
            logger.error(f"Ошибка сбора статистики: {e}")

# Глобальный экземпляр для использования в приложении
async_bot_handler = None

async def initialize_async_bot_handler(bot_token: str):
    """Инициализация асинхронного обработчика"""
    global async_bot_handler
    async_bot_handler = AsyncBotHandler(bot_token)
    await async_bot_handler.start()

async def shutdown_async_bot_handler():
    """Завершение работы асинхронного обработчика"""
    global async_bot_handler
    if async_bot_handler:
        await async_bot_handler.stop()

logger.info("Модуль асинхронного обработчика бота загружен")