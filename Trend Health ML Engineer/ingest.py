import sqlite3
import pandas as pd

DB_PATH = 'nyc311.db'
CSV_PATH = 'nyc_311_q1_2023.csv'
CHUNK_SIZE = 10000


def initialize_db():
    """Executes schema.sql to set up the SQLite DB"""
    with sqlite3.connect(DB_PATH) as conn, open('schema.sql', 'r') as f:
        conn.executescript(f.read())


def get_or_create_id(conn, table, lookup_cols, values):
    """
    Fetches an ID from a normalized table (complaint_types or boroughs),
    or inserts the record and returns the new ID.
    """
    where_clause = ' AND '.join([f"{col} = ?" for col in lookup_cols])
    select_sql = f"SELECT id FROM {table} WHERE {where_clause}"
    insert_sql = f"INSERT INTO {table} ({', '.join(lookup_cols)}) VALUES ({', '.join(['?' for _ in lookup_cols])})"

    cur = conn.cursor()
    cur.execute(select_sql, values)
    row = cur.fetchone()

    if row:
        return row[0]
    else:
        cur.execute(insert_sql, values)
        return cur.lastrowid


def clean_chunk(df):
    """Renames and selects needed columns, drops rows with missing keys"""
    needed = [
        'Unique Key', 'Created Date', 'Closed Date', 'Agency',
        'Complaint Type', 'Descriptor', 'Location Type', 'Incident Zip',
        'City', 'Borough', 'Status', 'Resolution Description',
        'Latitude', 'Longitude'
    ]

    df = df[needed].copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    df.dropna(subset=['unique_key'], inplace=True)
    return df


def ingest_data():
    """Loads CSV data into SQLite DB with foreign keys and deduplication"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        chunk = clean_chunk(chunk)

        for _, row in chunk.iterrows():
            # Insert or fetch complaint_type_id
            complaint_type_id = get_or_create_id(
                conn,
                table='complaint_types',
                lookup_cols=['complaint_type', 'descriptor'],
                values=[row['complaint_type'], row['descriptor']]
            )

            # Insert or fetch borough_id
            borough_id = get_or_create_id(
                conn,
                table='boroughs',
                lookup_cols=['borough'],
                values=[row['borough']]
            )

            # Check if request already exists
            cur.execute("SELECT 1 FROM service_requests WHERE unique_key = ?", (row['unique_key'],))
            if cur.fetchone():
                continue  # Skip if already exists

            # Insert into service_requests
            cur.execute("""
                INSERT INTO service_requests (
                    unique_key, created_date, closed_date, agency,
                    complaint_type_id, location_type, incident_zip,
                    city, borough_id, status, resolution_description,
                    latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['unique_key'], row['created_date'], row['closed_date'], row['agency'],
                complaint_type_id, row['location_type'], row['incident_zip'],
                row['city'], borough_id, row['status'], row['resolution_description'],
                row['latitude'], row['longitude']
            ))

        conn.commit()
        print(f"Inserted batch of {len(chunk)} rows...")

    conn.close()
    print("✅ Ingestion complete.")


if __name__ == "__main__":
    initialize_db()
    ingest_data()
