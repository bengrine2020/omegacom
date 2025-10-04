import csv
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST", "localhost")
PORT = int(os.getenv("DB_PORT", 3306))
NAME=os.getenv("DB_NAME")

folder="/srv/sftpuser/files"
TABLE_NAME = "parking_records"  # your table
CSV_FILE = "yellowbrick.csv"

MATCH_FIELDS = ["vehicle_plate", "start_time"]  # used to detect existing rows

# Connect to MariaDB
conn = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=NAME
)
cursor = conn.cursor(dictionary=True)


path=os.path.join(folder, CSV_FILE)

# --- Read CSV ---
with open(path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    csv_rows = list(reader)

# --- 1. Collect keys from CSV ---
csv_keys = set()
for row in csv_rows:
    key = tuple(row[f] for f in MATCH_FIELDS)
    csv_keys.add(key)

# --- 2. Get existing keys from DB ---
cursor.execute(f"SELECT {', '.join(MATCH_FIELDS)} FROM {TABLE_NAME}")
db_keys = {tuple(str(r[f]) for f in MATCH_FIELDS) for r in cursor.fetchall()}

# --- 3. Prepare INSERT/UPDATE ---
insert_update_query = f"""
INSERT INTO {TABLE_NAME} (
    collaborator, vehicle_plate, vehicle_label, location, park,
    user_annotation_label1, user_annotation_value1,
    user_annotation_label2, user_annotation_value2,
    user_type, start_time, end_time, duration, total_amount, currency,
    parking_fee, parking_fee_tax_included, parking_tax_rate, parking_VAT_number,
    service_fee, service_tax_included, service_tax_rate, service_VAT_number,
    confirmation_fee, confirmation_tax_included, confirmation_tax_rate, confirmation_VAT_number,
    reminder_fee, reminder_tax_included, reminder_tax_rate, reminder_VAT_number
)
VALUES (
    %(collaborator)s, %(vehicle_plate)s, %(vehicle_label)s, %(location)s, %(park)s,
    %(user_annotation_label1)s, %(user_annotation_value1)s,
    %(user_annotation_label2)s, %(user_annotation_value2)s,
    %(user_type)s, %(start_time)s, %(end_time)s, %(duration)s, %(total_amount)s, %(currency)s,
    %(parking_fee)s, %(parking_fee_tax_included)s, %(parking_tax_rate)s, %(parking_VAT_number)s,
    %(service_fee)s, %(service_tax_included)s, %(service_tax_rate)s, %(service_VAT_number)s,
    %(confirmation_fee)s, %(confirmation_tax_included)s, %(confirmation_tax_rate)s, %(confirmation_VAT_number)s,
    %(reminder_fee)s, %(reminder_tax_included)s, %(reminder_tax_rate)s, %(reminder_VAT_number)s
)
ON DUPLICATE KEY UPDATE
    collaborator=VALUES(collaborator),
    vehicle_label=VALUES(vehicle_label),
    location=VALUES(location),
    park=VALUES(park),
    user_annotation_label1=VALUES(user_annotation_label1),
    user_annotation_value1=VALUES(user_annotation_value1),
    user_annotation_label2=VALUES(user_annotation_label2),
    user_annotation_value2=VALUES(user_annotation_value2),
    user_type=VALUES(user_type),
    end_time=VALUES(end_time),
    duration=VALUES(duration),
    total_amount=VALUES(total_amount),
    currency=VALUES(currency),
    parking_fee=VALUES(parking_fee),
    parking_fee_tax_included=VALUES(parking_fee_tax_included),
    parking_tax_rate=VALUES(parking_tax_rate),
    parking_VAT_number=VALUES(parking_VAT_number),
    service_fee=VALUES(service_fee),
    service_tax_included=VALUES(service_tax_included),
    service_tax_rate=VALUES(service_tax_rate),
    service_VAT_number=VALUES(service_VAT_number),
    confirmation_fee=VALUES(confirmation_fee),
    confirmation_tax_included=VALUES(confirmation_tax_included),
    confirmation_tax_rate=VALUES(confirmation_tax_rate),
    confirmation_VAT_number=VALUES(confirmation_VAT_number),
    reminder_fee=VALUES(reminder_fee),
    reminder_tax_included=VALUES(reminder_tax_included),
    reminder_tax_rate=VALUES(reminder_tax_rate),
    reminder_VAT_number=VALUES(reminder_VAT_number)
"""

# Insert or update rows from CSV
inserted = 0
updated = 0
for row in csv_rows:
    # Convert boolean-like strings
    for bool_col in [
        "parking_fee_tax_included", "service_tax_included",
        "confirmation_tax_included", "reminder_tax_included"
    ]:
        if bool_col in row:
            val = str(row[bool_col]).strip().lower()
            row[bool_col] = val in ["1", "true", "yes", "y"]

    cursor.execute(insert_update_query, row)
    if cursor.rowcount == 1:
        inserted += 1
    elif cursor.rowcount == 2:
        updated += 1

# --- 4. Delete rows not in CSV ---
to_delete = db_keys - csv_keys
if to_delete:
    delete_query = f"DELETE FROM {TABLE_NAME} WHERE vehicle_plate=%s AND start_time=%s"
    cursor.executemany(delete_query, list(to_delete))
    print(f"🗑 Deleted {cursor.rowcount} old rows.")

conn.commit()

print(f"✅ Inserted {inserted} new rows, updated {updated} existing rows from '{CSV_FILE}'.")

cursor.close()
conn.close()
