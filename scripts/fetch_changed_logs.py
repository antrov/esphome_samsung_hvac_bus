import psycopg2
import json
import sys
from datetime import datetime, timedelta
import time

# Lista kluczy do ukrycia
HIDDEN_KEYS = ['#2708', '202', '2158', '22f7', '22fb', '22fc', '22fd', '22fe', '22ff', '243a', '24fb', '24fc', '4028', '402a', '402e', '4067', '4089', '408a', '4093', '4094', '40c4', '40c6', '4202', '4204', '4205', '4206', '420c', '4211', '4236', '4237', '4238', '4239', '4273', '4274', '4275', '4276', '4277', '4278', '4279', '427a', '427b', '427f', '428c', '42d4', '42d8', '42d9', '42e8', '42e9', '4401', '440e', '4423', '4424', '4426', '4427', '616d', '8001', '800d', '8010', '801a', '8032', '8033', '805e', '8061', '8077', '807c', '80af', '8204', '820a', '8217', '8218', '8223', '8229', '8236', '8237', '8238', '8239', '823b', '823d', '8247', '8248', '8249', '824b', '824c', '8254', '8280', '82b6', '82d9', '82db', '82ed', '840f', '8411', '8413', '8414']

# Funkcja do wczytywania konfiguracji bazy danych z pliku
def load_db_config(config_path="db_config.json"):
    with open(config_path, "r") as file:
        return json.load(file)

def fetch_changed_logs(conn, time_window_minutes):
    """
    Pobiera logi, których wartości zmieniły się w zadanym oknie czasowym.
    """
    time_threshold = datetime.now() - timedelta(minutes=time_window_minutes)
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT key, value, timestamp
            FROM logs
            WHERE timestamp >= %s
            ORDER BY timestamp ASC
        """, (time_threshold,))
        return cursor.fetchall()

def format_timeline(changed_logs, hide_keys):
    """
    Formatuje zmienione logi jako poziomy timeline na konsoli.
    Oś Y to klucze, oś X to czas, a wartości są wypisywane w odpowiednich miejscach.
    Jeśli hide_keys jest ustawione na True, ukrywa klucze z listy HIDDEN_KEYS.
    """
    MAX_WIDTH = 10  # Maksymalna szerokość kolumny dla wartości i nagłówków
    timeline = {}  # Słownik: {key: [(minute, value)]}
    times_set = set()

    for key, value, timestamp in changed_logs:
        if hide_keys and key in HIDDEN_KEYS:
            continue
        minute = timestamp.replace(second=0, microsecond=0)
        times_set.add(minute)
        if key not in timeline:
            timeline[key] = []
        timeline[key].append((minute, value))

    times = sorted(times_set)
    keys = sorted(timeline.keys())

    # Nagłówek osi X (czas)
    header = f"{' ':>{MAX_WIDTH}} | " + " | ".join([f"{time.strftime('%H:%M'):>{MAX_WIDTH}}" for time in times])
    result = [header]
    result.append("-" * len(header))

    # Wiersze dla każdego klucza (oś Y)
    for key in keys:
        row = [f"{key:>{MAX_WIDTH}}"]
        key_times = {minute: value for minute, value in timeline[key]}
        for time in times:
            if time in key_times:
                value = str(key_times[time])
                if len(value) > MAX_WIDTH:
                    value = value[:MAX_WIDTH - 3] + "..."
                row.append(f"{value:>{MAX_WIDTH}}")
            else:
                row.append(" " * MAX_WIDTH)
        result.append(" | ".join(row))

    return "\n".join(result)

def main(time_window_minutes, hide_keys):
    try:
        # Wczytanie konfiguracji bazy danych
        db_config = load_db_config()
        # Nawiązanie połączenia z bazą danych
        conn = psycopg2.connect(**db_config)

        while True:
            # Pobranie zmienionych logów w zadanym oknie czasowym
            changed_logs = fetch_changed_logs(conn, time_window_minutes)

            if changed_logs:
                # Wyczyszczenie konsoli
                sys.stdout.write("\033c")
                sys.stdout.flush()
                print("\nZmiany w logach:")
                print(format_timeline(changed_logs, hide_keys))
            else:
                print("\nBrak zmian w zadanym oknie czasowym.")

            # Odczekaj 1 minutę przed kolejnym sprawdzeniem
            for remaining in range(10, 0, -1):
                sys.stdout.write(f"\rOdświeżenie za {remaining} sekund... ")
                sys.stdout.flush()
                time.sleep(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Użycie: python fetch_changed_logs.py <time_window_minutes> [--hide-keys]")
        sys.exit(1)

    try:
        time_window_minutes = int(sys.argv[1])
        hide_keys = "--hide-keys" in sys.argv
        main(time_window_minutes, hide_keys)
    except ValueError:
        print("Podaj poprawną liczbę minut jako parametr.")
        sys.exit(1)

# 07.01 15:14 UTC - zmiana nastaw -1 -> +0.5 - zmieniło się 4248 na 5, 
# 15:19 0, 4248=0, 
# 15:21 1, 4248=10
# 2 = 20
# -1 = 65526
# -0.5 = 65531
# - 1.5 = 65521
# -2 = 65516

# 4000 -> 0, wyłączenie zone 1, 1 włączenie
# 411e zone 2
# 4065 CWU

# 4001 - ręczny heat mode 4, cool mode 1, auto mode 0
# 4002 - ręczny heat mode

# 4247 - ręczne zone 1 heat mode 27 -> 270, 28 -> 280, 25.5 -> 255
# 42d7 - ręczne zone 2 heat mode 30.5 -> 305

# 4235 - cwu, 42 -> 420
# 4066 - cwu mode