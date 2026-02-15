import serial
import serial.tools.list_ports
import time
import threading
from datetime import datetime
import re

class ArduinoReader:
    def __init__(self, port=None, baud_rate=9600):
        self.port = port
        self.baud_rate = baud_rate
        self.serial_connection = None
        self.is_connected = False
        self.callback = None
        self.running = False
        self.thread = None
        
        # Если порт не указан, пытаемся найти Arduino
        if port is None:
            self.port = self.find_arduino_port()
    
    def find_arduino_port(self):
        """Автоматически найти порт Arduino"""
        ports = serial.tools.list_ports.comports()
        
        for port in ports:
            # Arduino обычно имеет эти описания
            if 'Arduino' in port.description or 'USB Serial' in port.description:
                return port.device
            
            # На Linux часто ttyUSB0 или ttyACM0
            if 'ttyUSB' in port.device or 'ttyACM' in port.device:
                return port.device
        
        # Если не нашли, возвращаем стандартный порт
        return '/dev/ttyUSB0'  # Для Linux/Raspberry Pi
        # Для Windows: return 'COM3'
    
    def connect(self):
        """Подключение к Arduino"""
        try:
            self.serial_connection = serial.Serial(
                port=self.port,
                baudrate=self.baud_rate,
                timeout=2,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS
            )
            
            # Ждем инициализации Arduino
            time.sleep(2)
            
            # Очищаем буфер
            self.serial_connection.reset_input_buffer()
            
            self.is_connected = True
            print(f"✅ Connected to Arduino on {self.port}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Arduino: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Отключение от Arduino"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        
        if self.serial_connection and self.serial_connection.is_open:
            self.serial_connection.close()
        
        self.is_connected = False
        print("🔌 Disconnected from Arduino")
    
    def set_callback(self, callback_func):
        """Установить функцию обратного вызова для новых данных"""
        self.callback = callback_func
    
    def start_reading(self):
        """Запустить непрерывное чтение данных"""
        if not self.is_connected:
            if not self.connect():
                return False
        
        self.running = True
        self.thread = threading.Thread(target=self._read_loop, daemon=True)
        self.thread.start()
        return True
    
    def _read_loop(self):
        """Основной цикл чтения данных"""
        while self.running:
            try:
                if self.serial_connection and self.serial_connection.in_waiting:
                    line = self.serial_connection.readline().decode('utf-8', errors='ignore').strip()
                    
                    if line:
                        # Парсим данные из Arduino
                        parsed_data = self._parse_data(line)
                        
                        if parsed_data and self.callback:
                            self.callback(parsed_data)
                
                time.sleep(0.1)  # Небольшая задержка для снижения нагрузки
                
            except Exception as e:
                print(f"Error reading from Arduino: {e}")
                self.is_connected = False
                time.sleep(5)  # Ждем перед попыткой переподключения
    
    def _parse_data(self, line):
        """Парсинг данных из Arduino"""
        # Ожидаемый формат: "PM1:10.2,PM2.5:25.1,PM10:30.5,TEMP:22.5,HUM:45.0"
        try:
            data = {}
            
            # Простой парсер для формата ключ:значение
            parts = line.split(',')
            for part in parts:
                if ':' in part:
                    key, value = part.split(':', 1)
                    key = key.strip().upper()
                    value = value.strip()
                    
                    # Пытаемся конвертировать в число
                    try:
                        if key == 'PM1' or key == 'PM1.0':
                            data['pm1'] = float(value)
                        elif key == 'PM25' or key == 'PM2.5':
                            data['pm25'] = float(value)
                        elif key == 'PM10':
                            data['pm10'] = float(value)
                        elif key == 'TEMP' or key == 'TEMPERATURE':
                            data['temperature'] = float(value)
                        elif key == 'HUM' or key == 'HUMIDITY':
                            data['humidity'] = float(value)
                    except ValueError:
                        pass
            
            # Проверяем, что получили все необходимые данные
            required = ['pm25', 'temperature', 'humidity']
            if all(key in data for key in required):
                # Добавляем время
                data['timestamp'] = datetime.now().isoformat()
                return data
            else:
                return None
                
        except Exception as e:
            print(f"Parse error: {e}")
            return None
    
    def send_command(self, command):
        """Отправить команду Arduino"""
        if self.is_connected and self.serial_connection:
            try:
                self.serial_connection.write(f"{command}\n".encode())
                return True
            except Exception as e:
                print(f"Error sending command: {e}")
        return False
    
    def get_status(self):
        """Получить статус подключения"""
        return {
            'connected': self.is_connected,
            'port': self.port,
            'baud_rate': self.baud_rate
        }