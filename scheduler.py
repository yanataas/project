import threading
import time
from datetime import datetime, timedelta
import sqlite3
from database import AirQualityDatabase

class DataScheduler:
    def __init__(self, db, arduino_reader, app=None):
        self.db = db
        self.arduino = arduino_reader
        self.app = app
        self.running = False
        self.thread = None
        self.current_hour_data = []
        self.last_hourly_sample = None
        
        # Устанавливаем callback для Arduino
        self.arduino.set_callback(self.on_new_data)
    
    def on_new_data(self, data):
        """Обработка новых данных от Arduino"""
        # Сохраняем в базу
        self.db.save_reading(
            pm1=data.get('pm1'),
            pm25=data.get('pm25'),
            pm10=data.get('pm10'),
            temperature=data.get('temperature'),
            humidity=data.get('humidity')
        )
        
        # Добавляем в текущий часовой буфер
        self.current_hour_data.append(data)
        
        # Отправляем через SocketIO если есть приложение
        if self.app:
            with self.app.app_context():
                from flask_socketio import SocketIO
                socketio = SocketIO(self.app)
                socketio.emit('sensor_data', {
                    'time': datetime.now().strftime('%H:%M:%S'),
                    'status': 'collecting',
                    'accumulated_count': len(self.current_hour_data)
                })
    
    def calculate_hourly_average(self):
        """Вычислить среднее за час"""
        if not self.current_hour_data:
            return None
        
        # Собираем все значения
        pm1_vals = [d.get('pm1') for d in self.current_hour_data if d.get('pm1') is not None]
        pm25_vals = [d.get('pm25') for d in self.current_hour_data if d.get('pm25') is not None]
        pm10_vals = [d.get('pm10') for d in self.current_hour_data if d.get('pm10') is not None]
        temp_vals = [d.get('temperature') for d in self.current_hour_data if d.get('temperature') is not None]
        hum_vals = [d.get('humidity') for d in self.current_hour_data if d.get('humidity') is not None]
        
        # Вычисляем средние
        result = {
            'timestamp': datetime.now().replace(minute=0, second=0, microsecond=0).isoformat(),
            'pm1_avg': sum(pm1_vals) / len(pm1_vals) if pm1_vals else None,
            'pm25_avg': sum(pm25_vals) / len(pm25_vals) if pm25_vals else None,
            'pm10_avg': sum(pm10_vals) / len(pm10_vals) if pm10_vals else None,
            'temperature_avg': sum(temp_vals) / len(temp_vals) if temp_vals else None,
            'humidity_avg': sum(hum_vals) / len(hum_vals) if hum_vals else None,
            'sample_count': len(self.current_hour_data)
        }
        
        # Вычисляем AQI
        if result['pm25_avg']:
            result['aqi_avg'] = self.db.calculate_aqi(result['pm25_avg'])
        else:
            result['aqi_avg'] = None
        
        return result
    
    def save_hourly_sample(self):
        """Сохранить часовую выборку"""
        hourly_avg = self.calculate_hourly_average()
        
        if hourly_avg:
            # Сохраняем в базу
            self.db.save_hourly_average(
                timestamp=hourly_avg['timestamp'],
                pm1_avg=hourly_avg['pm1_avg'],
                pm25_avg=hourly_avg['pm25_avg'],
                pm10_avg=hourly_avg['pm10_avg'],
                temp_avg=hourly_avg['temperature_avg'],
                hum_avg=hourly_avg['humidity_avg'],
                aqi_avg=hourly_avg['aqi_avg'],
                sample_count=hourly_avg['sample_count']
            )
            
            self.last_hourly_sample = hourly_avg
            
            # Отправляем через SocketIO
            if self.app:
                with self.app.app_context():
                    from flask_socketio import SocketIO
                    socketio = SocketIO(self.app)
                    
                    # Определяем качество воздуха
                    aqi = hourly_avg['aqi_avg']
                    if aqi is None:
                        quality = 'Unknown'
                    elif aqi <= 50:
                        quality = 'Good'
                    elif aqi <= 100:
                        quality = 'Moderate'
                    elif aqi <= 150:
                        quality = 'Unhealthy'
                    else:
                        quality = 'Hazardous'
                    
                    socketio.emit('hourly_sample', {
                        'timestamp': hourly_avg['timestamp'],
                        'pm25': round(hourly_avg['pm25_avg'], 1) if hourly_avg['pm25_avg'] else '--',
                        'pm1': round(hourly_avg['pm1_avg'], 1) if hourly_avg['pm1_avg'] else '--',
                        'pm10': round(hourly_avg['pm10_avg'], 1) if hourly_avg['pm10_avg'] else '--',
                        'temperature': round(hourly_avg['temperature_avg'], 1) if hourly_avg['temperature_avg'] else '--',
                        'humidity': round(hourly_avg['humidity_avg'], 1) if hourly_avg['humidity_avg'] else '--',
                        'aqi': aqi if aqi else '--',
                        'quality': quality,
                        'sample_count': hourly_avg['sample_count'],
                        'time': datetime.now().strftime('%H:%M:%S')
                    })
        
        # Очищаем буфер
        self.current_hour_data = []
    
    def start(self):
        """Запустить планировщик"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print("📊 Scheduler started")
    
    def stop(self):
        """Остановить планировщик"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        print("📊 Scheduler stopped")
    
    def _run(self):
        """Основной цикл планировщика"""
        while self.running:
            now = datetime.now()
            
            # Проверяем, нужно ли сохранить часовую выборку
            # Сохраняем в начале каждого часа
            if now.minute == 0 and now.second < 10:
                self.save_hourly_sample()
                time.sleep(1)  # Чтобы не сохранять несколько раз
            
            # Также проверяем, не прошёл ли час с последнего сохранения
            if self.last_hourly_sample:
                last_time = datetime.fromisoformat(self.last_hourly_sample['timestamp'])
                if (now - last_time).total_seconds() >= 3600:
                    self.save_hourly_sample()
            
            time.sleep(1)  # Проверяем каждую секунду
    
    def get_current_progress(self):
        """Получить текущий прогресс сбора данных"""
        if not self.current_hour_data:
            return {
                'samples_collected': 0,
                'remaining': 3600,
                'progress': 0
            }
        
        # Время первого измерения в текущем часе
        first_sample = datetime.fromisoformat(self.current_hour_data[0]['timestamp'])
        now = datetime.now()
        
        elapsed = (now - first_sample).total_seconds()
        remaining = max(0, 3600 - elapsed)
        progress = min(100, (elapsed / 3600) * 100)
        
        return {
            'samples_collected': len(self.current_hour_data),
            'remaining': int(remaining),
            'progress': round(progress, 1)
        }