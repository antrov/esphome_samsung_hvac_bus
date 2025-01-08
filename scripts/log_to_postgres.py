import subprocess
import psycopg2
import json
import re
import sys
from datetime import datetime

# Prefix do filtrowania logów
LOG_PREFIX = ">"
# Komenda, którą skrypt będzie wywoływał
COMMAND = "uvx esphome logs esphome_samsung_hvac_bus.yaml"

# Funkcja do wczytywania konfiguracji bazy danych z pliku
def load_db_config(config_path="db_config.json"):
    with open(config_path, "r") as file:
        return json.load(file)

def create_table_if_not_exists(conn):
    """
    Tworzy tabelę logs, jeśli nie istnieje.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        """)
    conn.commit()

def get_previous_value(conn, key):
    """
    Pobiera ostatnią wartość dla danego klucza z bazy danych.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT value FROM logs
            WHERE key = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (key,))
        result = cursor.fetchone()
        return result[0] if result else None

def insert_log_to_db(conn, key, value, timestamp):
    """
    Wstawia nowy log do bazy danych.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO logs (key, value, timestamp)
            VALUES (%s, %s, %s)
        """, (key, value, timestamp))
    conn.commit()

def process_logs():
    try:
        # Wczytanie konfiguracji bazy danych
        db_config = load_db_config()
        # Nawiązanie połączenia z bazą danych
        conn = psycopg2.connect(**db_config)

        # Tworzenie tabeli, jeśli nie istnieje
        create_table_if_not_exists(conn)

        # Uruchomienie komendy i czytanie jej wyjścia w czasie rzeczywistym
        process = subprocess.Popen(COMMAND, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        log_pattern = re.compile(r">\s+\S+\s+(\S+)\s*=\s*(\S+)")
        found_first_match = False

        for line in process.stdout:
            line = line.strip()
            if not found_first_match:
                print(line)
            if line.startswith(LOG_PREFIX):
                match = log_pattern.match(line)
                if match:
                    found_first_match = True
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    timestamp = datetime.now()

                    # Pobranie poprzedniej wartości dla klucza
                    previous_value = get_previous_value(conn, key)
                    if previous_value is None or previous_value != value:
                        # Zapisanie logu do bazy danych tylko jeśli wartość się zmieniła
                        insert_log_to_db(conn, key, value, timestamp)
                        print(f"Inserted log: key={key}, value={value}, timestamp={timestamp}")
                else:
                    print(f"Invalid log format: {line}", file=sys.stderr)

        # Obsługa błędów procesu
        process.stdout.close()
        process.wait()
        if process.returncode != 0:
            error_message = process.stderr.read()
            print(f"Command failed with error: {error_message}", file=sys.stderr)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_logs()
