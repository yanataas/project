from flask import Flask, render_template, jsonify, send_file, request
from flask_socketio import SocketIO, emit
from database import AirQualityDatabase
from arduino_reader import ArduinoReader
from scheduler import DataScheduler
import os
import threading
from datetime import datetime

app = Flask(__name__, static_folder='static', static_url_path='')
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")

# Инициализация компонентов
db = AirQualityDatabase()
arduino = ArduinoReader()
scheduler = DataScheduler(db, arduino, app)

# Флаг для автоопределения Arduino
arduino_auto_connect = True

@app.route('/')
def index():
    """Главная страница"""
    return app.send_static_file('index.html')

@app.route('/api/hourly_samples')
def get_hourly_samples():
    """API для получения часовых выборок"""
    hours = request.args.get('hours', 168, type=int)
    data = db.get_hourly_samples(hours)
    return jsonify(data)

@app.route('/api/current_stats')
def get_current_stats():
    """API для текущей статистики"""
    stats = db.get_current_hour_stats()
    return jsonify(stats)

@app.route('/api/current_progress')
def get_current_progress():
    """API для прогресса сбора данных"""
    progress = scheduler.get_current_progress()
    return jsonify(progress)

@app.route('/api/export/last_7days')
def export_last_7days():
    """Экспорт данных за последние 7 дней"""
    csv_path = db.export_last_7days()
    if os.path.exists(csv_path):
        return send_file(csv_path, as_attachment=True, 
                        download_name=f'air_quality_{datetime.now().strftime("%Y%m%d")}.csv')
    return jsonify({'error': 'No data available'}), 404

@app.route('/api/long_term_stats')
def get_long_term_stats():
    """Статистика за всё время"""
    stats = db.get_long_term_stats()
    return jsonify(stats)

@app.route('/api/arduino/status')
def get_arduino_status():
    """Статус подключения Arduino"""
    status = arduino.get_status()
    return jsonify(status)

@app.route('/api/arduino/connect', methods=['POST'])
def connect_arduino():
    """Подключение к Arduino"""
    port = request.json.get('port') if request.is_json else None
    if port:
        arduino.port = port
    
    success = arduino.connect()
    if success:
        arduino.start_reading()
        return jsonify({'success': True, 'message': 'Connected to Arduino'})
    else:
        return jsonify({'success': False, 'message': 'Failed to connect'})

@app.route('/api/arduino/disconnect', methods=['POST'])
def disconnect_arduino():
    """Отключение от Arduino"""
    arduino.disconnect()
    return jsonify({'success': True, 'message': 'Disconnected'})

@socketio.on('connect')
def handle_connect():
    """Обработка подключения клиента"""
    print('Client connected')
    emit('connected', {'message': 'Connected to server'})

@socketio.on('disconnect')
def handle_disconnect():
    """Обработка отключения клиента"""
    print('Client disconnected')

def start_background_tasks():
    """Запуск фоновых задач"""
    # Подключаемся к Arduino и запускаем сбор данных
    if arduino_auto_connect:
        if arduino.connect():
            arduino.start_reading()
            scheduler.start()
            print("✅ Arduino connected and scheduler started")
        else:
            print("⚠️ Arduino not found. Will retry later...")
            
            # Пытаемся переподключиться через 30 секунд
            def retry_connection():
                import time
                time.sleep(30)
                if not arduino.is_connected:
                    print("🔄 Retrying Arduino connection...")
                    if arduino.connect():
                        arduino.start_reading()
                        scheduler.start()
            
            threading.Thread(target=retry_connection, daemon=True).start()

if __name__ == '__main__':
    print("=" * 50)
    print("🌬️  Air Quality Monitor Server")
    print("=" * 50)
    print(f"📁 Database: {db.db_path}")
    print(f"🔌 Arduino port: {arduino.port or 'Auto-detect'}")
    print("\n🚀 Starting server...")
    
    # Запускаем фоновые задачи
    start_background_tasks()
    
    # Запускаем сервер
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
