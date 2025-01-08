import psycopg2
import json
import sys

def load_db_config(config_path="db_config.json"):
    """Wczytuje konfigurację bazy danych z pliku JSON."""
    with open(config_path, "r") as file:
        return json.load(file)

def fetch_duplicate_keys(conn):
    """Pobiera listę kluczy, które pojawiły się więcej niż jeden raz w bazie danych."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT key
            FROM logs
            GROUP BY key
            HAVING COUNT(*) > 1
            ORDER BY key ASC
        """)
        result = cursor.fetchall()
        return [row[0] for row in result]

def main():
    try:
        # Wczytanie konfiguracji bazy danych
        db_config = load_db_config()
        # Nawiązanie połączenia z bazą danych
        conn = psycopg2.connect(**db_config)

        # Pobranie listy kluczy z wieloma wystąpieniami
        duplicate_keys = fetch_duplicate_keys(conn)

        # Wyświetlenie listy kluczy w formacie Python list
        print("Lista kluczy z wieloma wystąpieniami:")
        print(duplicate_keys)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
