import asyncio
import aiofiles
import aiohttp
import concurrent.futures
import multiprocessing as mp
from multiprocessing import Pool, Manager, Lock
import threading
import time
import os
import json
import psutil
import gc
from typing import Dict, List, Any, Optional
from functools import lru_cache
import logging
from datetime import datetime
from cachetools import TTLCache
# import numpy as np  # Отключено для совместимости
from PIL import Image
import io

# Настройка логирования для многопроцессорности
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(processName)s - %(message)s')
logger = logging.getLogger(__name__)

class HighPerformanceFileHandler:
    """Высокопроизводительная обработка файлов с мультипроцессингом"""
    
    def __init__(self, max_workers=None, max_file_size=50*1024*1024):  # 50MB лимит
        self.max_workers = max_workers or min(8, (os.cpu_count() or 1) * 2)  # Уменьшено для Windows
        self.max_file_size = max_file_size
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self.process_pool = None  # Инициализируем позже, когда нужно
        self.cache = TTLCache(maxsize=1000, ttl=300)  # 5 минут кеш
        
        # Создание менеджера для разделяемых данных - отложенная инициализация
        self.manager = None
        self.shared_data = {}
        self.lock = threading.Lock()  # Используем threading.Lock вместо multiprocessing.Lock
        
        logger.info(f"Инициализирован обработчик файлов с {self.max_workers} потоками")
    
    async def process_file_async(self, file_path: str, operation: str = 'read') -> Optional[Any]:
        """Асинхронная обработка файла"""
        try:
            # Проверка размера файла
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            if file_size > self.max_file_size:
                logger.warning(f"Файл {file_path} превышает лимит размера ({file_size} > {self.max_file_size})")
                return None
            
            # Кеширование для часто используемых файлов
            cache_key = f"{file_path}:{operation}:{os.path.getmtime(file_path) if os.path.exists(file_path) else 0}"
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            if operation == 'read':
                result = await self._read_file_async(file_path)
            elif operation == 'write':
                result = await self._write_file_async(file_path)
            elif operation == 'process_image':
                result = await self._process_image_async(file_path)
            else:
                result = None
            
            if result is not None:
                self.cache[cache_key] = result
                
            return result
            
        except Exception as e:
            logger.error(f"Ошибка обработки файла {file_path}: {e}")
            return None
    
    async def _read_file_async(self, file_path: str) -> Optional[Dict]:
        """Асинхронное чтение JSON файла"""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content) if content.strip() else {}
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return {}
    
    async def _write_file_async(self, file_path: str, data: Dict) -> bool:
        """Асинхронная запись JSON файла с оптимизацией"""
        try:
            # Создание временного файла для атомарной записи
            temp_path = f"{file_path}.tmp"
            
            async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
                json_str = json.dumps(data, ensure_ascii=False, indent=2, separators=(',', ':'))
                await f.write(json_str)
                # Принудительная запись на диск (Windows compatible)
                try:
                    await f.fsync()
                except AttributeError:
                    # Fallback для Windows - файл будет записан при закрытии
                    pass
            
            # Атомарная замена файла
            if os.name == 'nt':  # Windows
                if os.path.exists(file_path):
                    os.remove(file_path)
                os.rename(temp_path, file_path)
            else:  # Unix
                os.rename(temp_path, file_path)
                
            return True
            
        except Exception as e:
            logger.error(f"Ошибка записи файла {file_path}: {e}")
            # Удаляем временный файл при ошибке
            if os.path.exists(f"{file_path}.tmp"):
                os.remove(f"{file_path}.tmp")
            return False
    
    async def _process_image_async(self, file_path: str) -> Optional[Dict]:
        """Асинхронная обработка изображений с оптимизацией"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(self.executor, self._process_image_sync, file_path)
            return result
        except Exception as e:
            logger.error(f"Ошибка обработки изображения {file_path}: {e}")
            return None
    
    def _process_image_sync(self, file_path: str) -> Dict:
        """Синхронная обработка изображения"""
        try:
            with Image.open(file_path) as img:
                # Определение оптимального размера
                original_size = img.size
                max_size = (1920, 1080)  # Full HD максимум
                
                # Сжатие если изображение слишком большое
                if img.size[0] > max_size[0] or img.size[1] > max_size[1]:
                    img.thumbnail(max_size, Image.Resampling.LANCZOS)
                
                # Оптимизация качества
                if img.format in ['JPEG', 'JPG']:
                    quality = 85
                elif img.format == 'PNG':
                    img = img.convert('RGB')  # Конвертируем PNG в RGB для сжатия
                    quality = 85
                else:
                    quality = 85
                
                # Сохранение оптимизированной версии
                optimized_path = file_path.replace('.', '_optimized.')
                img.save(optimized_path, optimize=True, quality=quality)
                
                return {
                    'original_size': original_size,
                    'optimized_size': img.size,
                    'optimized_path': optimized_path,
                    'compression_ratio': os.path.getsize(optimized_path) / os.path.getsize(file_path)
                }
                
        except Exception as e:
            logger.error(f"Ошибка синхронной обработки изображения: {e}")
            return {}

class MultiCoreDataProcessor:
    """Многоядерный процессор данных"""
    
    def __init__(self):
        self.cpu_count = os.cpu_count() or 1
        self.max_processes = min(8, self.cpu_count)
        self.chunk_size = max(1, 1000 // self.max_processes)
        
        logger.info(f"Инициализирован процессор с {self.max_processes} процессами")
    
    def process_data_parallel(self, data: List[Dict], operation_func, **kwargs) -> List[Any]:
        """Параллельная обработка данных"""
        if not data:
            return []
        
        try:
            # Для Windows используем ThreadPoolExecutor вместо Pool
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_processes) as executor:
                # Разделение данных на чанки
                chunks = [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
                
                # Применяем функцию к каждому чанку параллельно
                futures = [executor.submit(self._process_chunk, chunk, operation_func, kwargs) for chunk in chunks]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
            
            # Объединяем результаты
            flattened_results = []
            for chunk_result in results:
                if isinstance(chunk_result, list):
                    flattened_results.extend(chunk_result)
                else:
                    flattened_results.append(chunk_result)
            
            return flattened_results
            
        except Exception as e:
            logger.error(f"Ошибка параллельной обработки данных: {e}")
            # Fallback на однопоточную обработку
            return [operation_func(item, **kwargs) for item in data]
    
    @staticmethod
    def _process_chunk(chunk: List[Dict], operation_func, kwargs: Dict) -> List[Any]:
        """Обработка чанка данных"""
        try:
            return [operation_func(item, **kwargs) for item in chunk]
        except Exception as e:
            logger.error(f"Ошибка обработки чанка: {e}")
            return []

class MemoryOptimizer:
    """Оптимизатор памяти"""
    
    def __init__(self):
        self.memory_threshold = 0.8  # 80% использования памяти
        self.gc_interval = 30  # секунд между сборкой мусора
        self.last_gc = time.time()
    
    def check_memory_usage(self) -> Dict[str, float]:
        """Проверка использования памяти"""
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'used_gb': memory.used / (1024**3),
            'available_gb': memory.available / (1024**3),
            'percent_used': memory.percent
        }
    
    def optimize_memory(self, force: bool = False) -> bool:
        """Оптимизация памяти"""
        try:
            current_time = time.time()
            memory_info = self.check_memory_usage()
            
            # Принудительная или по времени/памяти сборка мусора
            if (force or 
                memory_info['percent_used'] > self.memory_threshold * 100 or
                current_time - self.last_gc > self.gc_interval):
                
                logger.info(f"Запуск сборки мусора. Память: {memory_info['percent_used']:.1f}%")
                
                # Принудительная сборка мусора
                collected = gc.collect()
                
                # Обновляем время последней сборки
                self.last_gc = current_time
                
                # Проверяем результат
                new_memory_info = self.check_memory_usage()
                freed_mb = (memory_info['used_gb'] - new_memory_info['used_gb']) * 1024
                
                logger.info(f"Сборка мусора завершена. Собрано {collected} объектов, "
                           f"освобождено ~{freed_mb:.1f}MB")
                
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Ошибка оптимизации памяти: {e}")
            return False

class AsyncDatabaseManager:
    """Асинхронный менеджер базы данных с кешированием"""
    
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.file_handler = HighPerformanceFileHandler()
        self.cache = TTLCache(maxsize=500, ttl=60)  # 1 минута кеш для БД операций
        self.lock = asyncio.Lock()
        
        # Создание директории если не существует
        os.makedirs(data_dir, exist_ok=True)
        
    async def load_data(self, filename: str) -> Dict:
        """Загрузка данных с кешированием"""
        file_path = os.path.join(self.data_dir, filename)
        
        # Проверяем кеш
        cache_key = f"load:{filename}"
        if cache_key in self.cache:
            return self.cache[cache_key].copy()
        
        # Загружаем из файла
        data = await self.file_handler.process_file_async(file_path, 'read') or {}
        
        # Кешируем результат
        self.cache[cache_key] = data.copy()
        
        return data
    
    async def save_data(self, filename: str, data: Dict) -> bool:
        """Сохранение данных с блокировкой"""
        file_path = os.path.join(self.data_dir, filename)
        
        async with self.lock:
            try:
                # Валидация размера данных
                json_str = json.dumps(data, ensure_ascii=False)
                data_size = len(json_str.encode('utf-8'))
                
                # Проверка на превышение размера файла
                if data_size > self.file_handler.max_file_size:
                    logger.warning(f"Данные для {filename} превышают максимальный размер")
                    # Вместо удаления всех данных, создаем архив старых данных
                    await self._archive_large_data(filename, data)
                    return False
                
                # Сохранение
                success = await self.file_handler._write_file_async(file_path, data)
                
                if success:
                    # Обновляем кеш
                    cache_key = f"load:{filename}"
                    self.cache[cache_key] = data.copy()
                
                return success
                
            except Exception as e:
                logger.error(f"Ошибка сохранения {filename}: {e}")
                return False
    
    async def _archive_large_data(self, filename: str, data: Dict) -> None:
        """Архивирование больших данных вместо удаления"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"{filename.replace('.json', '')}_archive_{timestamp}.json"
            archive_path = os.path.join(self.data_dir, 'archives')
            
            os.makedirs(archive_path, exist_ok=True)
            
            archive_file = os.path.join(archive_path, archive_filename)
            await self.file_handler._write_file_async(archive_file, data)
            
            logger.info(f"Данные {filename} заархивированы в {archive_filename}")
            
        except Exception as e:
            logger.error(f"Ошибка архивирования {filename}: {e}")

# Глобальные экземпляры для использования в приложении - ленивая инициализация
file_handler = None
data_processor = None
memory_optimizer = None
db_manager = None

def get_file_handler():
    global file_handler
    if file_handler is None:
        file_handler = HighPerformanceFileHandler()
    return file_handler

def get_data_processor():
    global data_processor
    if data_processor is None:
        data_processor = MultiCoreDataProcessor()
    return data_processor

def get_memory_optimizer():
    global memory_optimizer
    if memory_optimizer is None:
        memory_optimizer = MemoryOptimizer()
    return memory_optimizer

def get_db_manager():
    global db_manager
    if db_manager is None:
        db_manager = AsyncDatabaseManager()
    return db_manager

# Декораторы для оптимизации
def memory_optimized(func):
    """Декоратор для автоматической оптимизации памяти"""
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            get_memory_optimizer().optimize_memory()
            return result
        except Exception as e:
            logger.error(f"Ошибка в функции {func.__name__}: {e}")
            raise
    return wrapper

def async_cached(ttl: int = 300):
    """Декоратор для кеширования асинхронных функций"""
    def decorator(func):
        cache = TTLCache(maxsize=100, ttl=ttl)
        
        async def wrapper(*args, **kwargs):
            # Создаем ключ кеша
            cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            if cache_key in cache:
                return cache[cache_key]
            
            result = await func(*args, **kwargs)
            cache[cache_key] = result
            return result
            
        return wrapper
    return decorator

# Утилиты для работы с производительностью
def get_system_performance() -> Dict[str, Any]:
    """Получение информации о производительности системы"""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'cpu_cores': psutil.cpu_count(),
            'cpu_usage_percent': cpu_percent,
            'memory_total_gb': memory.total / (1024**3),
            'memory_used_gb': memory.used / (1024**3),
            'memory_percent': memory.percent,
            'disk_total_gb': disk.total / (1024**3),
            'disk_used_gb': disk.used / (1024**3),
            'disk_percent': (disk.used / disk.total) * 100,
            'processes': len(psutil.pids())
        }
    except Exception as e:
        logger.error(f"Ошибка получения информации о системе: {e}")
        return {}

logger.info("Модуль оптимизации производительности загружен успешно")