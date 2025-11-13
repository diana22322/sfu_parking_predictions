import json
import os
import base64
import psycopg2
from datetime import datetime
from psycopg2 import sql

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration -- Environment variables
# completed in AWS lambda console
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

# lot capacity lookup
LOT_CAPACITIES = {
    'BURNABY': {'NORTH': 1000, 'EAST': 800, 'CENTRAL': 600, 'WEST': 500, 'SOUTH': 400},
    'SURREY': {'SRYC': 350, 'SRYE': 250}
}

FLAT_CAPACITIES = {
    lot_id: cap for campus in LOT_CAPACITIES.values() for lot_id, cap in campus.items()
}

def get_db_connection():
    # establishes and returns a PostgreSQL database connection
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        raise EnvironmentError("Database connection variables are not set.")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=5
    )

def process_event(event_data: dict, conn) -> None:
    # calculates the occupancy change and executes the atomic update in the database

    lot_id = event_data['lot_id']
    campus = event_data['campus']
    event_type = event_data['event_type']
    timestamp = event_data['timestamp'].replace('Z', '+00:00')

    occupancy_change = 1 if event_type == 'ARRIVAL' else -1

    # get the static capacity of the lot
    capacity = FLAT_CAPACITIES.get(lot_id, 0)

    if capacity == 0:
        print(f"Warning: Unknown lot ID {lot_id}. Skipping update")
        return

    initial_free_spots = capacity - occupancy_change
    initial_rate = round(occupancy_change / capacity, 4) if capacity > 0 else 0


    sql_query = sql.SQL("""
            INSERT INTO current_lot_occupancy 
                (lot_id, campus_name, capacity, current_occupancy, free_spots, occupancy_rate, last_updated_ts)
            VALUES 
                (
                    %s, %s, %s, %s, %s, %s, %s
                )
            ON CONFLICT (lot_id) DO UPDATE SET
                current_occupancy = current_lot_occupancy.current_occupancy + EXCLUDED.current_occupancy,
                free_spots = current_lot_occupancy.capacity - (current_lot_occupancy.current_occupancy + EXCLUDED.current_occupancy),
                occupancy_rate = (current_lot_occupancy.current_occupancy + EXCLUDED.current_occupancy)::NUMERIC / current_lot_occupancy.capacity,
                last_updated_ts = EXCLUDED.last_updated_ts
        """)

    # The EXCLUDED.current_occupancy here is just the single event's change (+1 or -1)
    # database handles the math atomically: current_value + (+1 or -1)

    with conn.cursor() as cur:
        cur.execute(sql_query, (
            lot_id,
            campus,
            capacity,
            occupancy_change,     # For INSERT: current_occupancy
            initial_free_spots,
            initial_rate,
            timestamp
        ))
    conn.commit()
    print(f"SUCCESS: {event_type} event processed for {lot_id}. Change: {occupancy_change}.")

def lambda_handler(kinesis_event, context):
    # Kinesis stream processor function. Reads records and updates PostgreSQL
    logger.info("psycopg2 loaded from: %s", psycopg2.__file__)
    records_processed = 0
    try:
        conn = get_db_connection()
        # process records from the Kinesis batch
        for record in kinesis_event['Records']:
            try:
                # kinesis data is base64 encoded
                payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                event_data = json.loads(payload)

                # process the individual event
                process_event(event_data, conn)

                records_processed += 1

            except Exception as e:
                print(f"Error processing single record: {e} | Record: {record.get('kinesis', {}).get('data')}")

    except EnvironmentError as e:
        print(f"FATAL: Configuration error. {e}")
        raise

    except Exception as e:
        print(f"FATAL: Database or connection error during batch: {e}")
        raise

    finally:
        # ensure connection is closed
        if 'conn' in locals() and conn:
            conn.close()

    print(f"Batch completed. Total records processed: {records_processed}")

    return f"Successfully processed {records_processed} records."
