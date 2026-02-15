import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
import os

class AirQualityDatabase:
    def __init__(self, db_path='data/air_quality.db'):
        # Создаем папку data если её нет
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Таблица для сырых данных (каждое измерение)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                pm1 REAL,
                pm25 REAL,
                pm10 REAL,
                temperature REAL,
                humidity REAL,
                aqi INTEGER
            )
        ''')
        
        # Таблица для часовых средних
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hourly_averages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                pm1_avg REAL,
                pm25_avg REAL,
                pm10_avg REAL,
                temperature_avg REAL,
                humidity_avg REAL,
                aqi_avg INTEGER,
                sample_count INTEGER,
                UNIQUE(timestamp)
            )
        ''')
        
        # Таблица для калибровки (если нужно будет корректировать данные)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS calibration (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                param_name TEXT,
                offset_value REAL,
                multiplier REAL,
                notes TEXT
            )
        ''')
        
        # Индексы для быстрого поиска
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_readings(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hourly_timestamp ON hourly_averages(timestamp)')
        
        conn.commit()
        conn.close()
    
    def save_reading(self, pm1, pm25, pm10, temperature, humidity, aqi=None):
        """Сохранить одно измерение"""
        if aqi is None:
            aqi = self.calculate_aqi(pm25)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO raw_readings (pm1, pm25, pm10, temperature, humidity, aqi)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (pm1, pm25, pm10, temperature, humidity, aqi))
        
        conn.commit()
        conn.close()
        return cursor.lastrowid
    
    def save_hourly_average(self, timestamp, pm1_avg, pm25_avg, pm10_avg, 
                           temp_avg, hum_avg, aqi_avg, sample_count):
        """Сохранить часовое среднее"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO hourly_averages 
            (timestamp, pm1_avg, pm25_avg, pm10_avg, temperature_avg, 
             humidity_avg, aqi_avg, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, pm1_avg, pm25_avg, pm10_avg, 
              temp_avg, hum_avg, aqi_avg, sample_count))
        
        conn.commit()
        conn.close()
    
    def get_hourly_samples(self, hours=168):  # 7 дней * 24 = 168 часов
        """Получить последние часовые средние"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT 
                timestamp,
                pm25_avg as pm25,
                pm1_avg as pm1,
                pm10_avg as pm10,
                temperature_avg as temperature,
                humidity_avg as humidity,
                aqi_avg as aqi,
                sample_count
            FROM hourly_averages 
            ORDER BY timestamp DESC 
            LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=(hours,))
        conn.close()
        
        if df.empty:
            return []
        
        # Преобразуем в формат для JSON
        result = []
        for _, row in df.iterrows():
            result.append({
                'timestamp': row['timestamp'],
                'pm25': round(row['pm25'], 1) if pd.notna(row['pm25']) else None,
                'pm1': round(row['pm1'], 1) if pd.notna(row['pm1']) else None,
                'pm10': round(row['pm10'], 1) if pd.notna(row['pm10']) else None,
                'temperature': round(row['temperature'], 1) if pd.notna(row['temperature']) else None,
                'humidity': round(row['humidity'], 1) if pd.notna(row['humidity']) else None,
                'aqi': int(row['aqi']) if pd.notna(row['aqi']) else None,
                'sample_count': int(row['sample_count']) if pd.notna(row['sample_count']) else 0
            })
        
        return result
    
    def get_current_hour_stats(self):
        """Получить статистику за текущий час"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT 
                COUNT(*) as sample_count,
                AVG(pm1) as pm1_avg,
                AVG(pm25) as pm25_avg,
                AVG(pm10) as pm10_avg,
                AVG(temperature) as temp_avg,
                AVG(humidity) as hum_avg,
                AVG(aqi) as aqi_avg,
                MIN(timestamp) as first_sample,
                MAX(timestamp) as last_sample
            FROM raw_readings 
            WHERE datetime(timestamp) >= datetime('now', '-1 hour')
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty or df.iloc[0]['sample_count'] == 0:
            return {'sample_count': 0, 'progress': 0}
        
        stats = df.iloc[0].to_dict()
        
        # Вычисляем прогресс (сколько процентов часа прошло)
        first_sample = datetime.fromisoformat(stats['first_sample'])
        last_sample = datetime.fromisoformat(stats['last_sample'])
        now = datetime.now()
        
        # Время, прошедшее с первого измерения
        elapsed = (now - first_sample).total_seconds()
        progress = min(100, (elapsed / 3600) * 100)
        
        # Оставшееся время
        remaining = max(0, 3600 - elapsed)
        
        return {
            'sample_count': int(stats['sample_count']),
            'pm1_avg': round(stats['pm1_avg'], 1) if pd.notna(stats['pm1_avg']) else None,
            'pm25_avg': round(stats['pm25_avg'], 1) if pd.notna(stats['pm25_avg']) else None,
            'pm10_avg': round(stats['pm10_avg'], 1) if pd.notna(stats['pm10_avg']) else None,
            'temperature_avg': round(stats['temp_avg'], 1) if pd.notna(stats['temp_avg']) else None,
            'humidity_avg': round(stats['hum_avg'], 1) if pd.notna(stats['hum_avg']) else None,
            'aqi_avg': int(stats['aqi_avg']) if pd.notna(stats['aqi_avg']) else None,
            'progress': round(progress, 1),
            'remaining': int(remaining),
            'first_sample': stats['first_sample'],
            'last_sample': stats['last_sample']
        }
    
    def calculate_aqi(self, pm25):
        """Расчет AQI по PM2.5 (US EPA стандарт)"""
        if pm25 is None:
            return None
        
        if pm25 <= 12.0:
            return int((50.0 / 12.0) * pm25)
        elif pm25 <= 35.4:
            return int(50 + (50.0 / 23.4) * (pm25 - 12.1))
        elif pm25 <= 55.4:
            return int(100 + (50.0 / 20.0) * (pm25 - 35.5))
        elif pm25 <= 150.4:
            return int(150 + (50.0 / 94.9) * (pm25 - 55.5))
        elif pm25 <= 250.4:
            return int(200 + (100.0 / 99.9) * (pm25 - 150.5))
        else:
            return int(300 + (200.0 / 249.9) * (min(pm25, 500.4) - 250.5))
    
    def export_last_7days(self):
        """Экспорт данных за последние 7 дней"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT * FROM hourly_averages 
            WHERE datetime(timestamp) >= datetime('now', '-7 days')
            ORDER BY timestamp
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return "No data available"
        
        # Сохраняем в CSV
        csv_path = 'data/export_last_7days.csv'
        df.to_csv(csv_path, index=False)
        return csv_path
    
    def get_long_term_stats(self):
        """Статистика за всё время измерений"""
        conn = sqlite3.connect(self.db_path)
        
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM raw_readings')
        total_readings = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM hourly_averages')
        total_hours = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT 
                MIN(timestamp), 
                MAX(timestamp) 
            FROM raw_readings
        ''')
        date_range = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_readings': total_readings,
            'total_hours': total_hours,
            'first_reading': date_range[0] if date_range[0] else None,
            'last_reading': date_range[1] if date_range[1] else None
        }