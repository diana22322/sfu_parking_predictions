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
    'BURNABY': {'NORTH': 2000, 'EAST': 1500, 'CENTRAL': 600, 'WEST': 500, 'SOUTH': 400},
    'SURREY': {'SRYC': 450, 'SRYE': 450}
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

def process_event(event_data: dict, cur) -> None:
    # calculates the occupancy change and executes the atomic update using the provided cursor
    # doesn't COMMIT transaction -- will do batch COMMITS later

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

    # INSERT logic uses GREATEST() to prevent inserting -1 for a new lot
    # UPDATE logic passes the raw `occupancy change` again to ensure atomic addition/subtraction, clamped b/w 0 and cap
    sql_query = sql.SQL("""
            INSERT INTO current_lot_occupancy 
                (lot_id, campus_name, capacity, current_occupancy, free_spots, occupancy_rate, last_updated_ts)
            VALUES 
                (
                    %s,  -- lot_id
                    %s,  -- campus
                    %s,  -- capacity
                    GREATEST(0, %s), -- INSERT: current_occupancy (e.g., GREATEST(0, -1) -> 0)
                    %s - GREATEST(0, %s), -- INSERT: free_spots
                    GREATEST(0, %s)::NUMERIC / %s, -- INSERT: occupancy_rate
                    %s  -- last_updated_ts
                )
            ON CONFLICT (lot_id) DO UPDATE SET
                -- UPDATE: uses the raw occupancy_change (param 10, 11, 12)
                -- this prevents current occupancy from being less than 0 or greater than capacity
                current_occupancy = GREATEST(0, LEAST(current_lot_occupancy.capacity, current_lot_occupancy.current_occupancy + %s)),

                free_spots = current_lot_occupancy.capacity - 
                    GREATEST(0, LEAST(current_lot_occupancy.capacity, current_lot_occupancy.current_occupancy + %s)),

                occupancy_rate = (GREATEST(0, LEAST(current_lot_occupancy.capacity, current_lot_occupancy.current_occupancy + %s))::NUMERIC / current_lot_occupancy.capacity),

                last_updated_ts = EXCLUDED.last_updated_ts
        """)

    # query has 12 parameters to safely handle both INSERT and UPDATE logic
    cur.execute(sql_query, (
        lot_id,
        campus,
        capacity,

        # Parameters for INSERT (VALUES) clause
        occupancy_change,  # param 4: for current_occupancy
        capacity,  # param 5: for free_spots
        occupancy_change,  # param 6: for free_spots
        occupancy_change,  # param 7: for occupancy_rate
        capacity,  # param 8: for occupancy_rate
        timestamp,  # param 9: for last_updated_ts

        # Parameters for UPDATE (ON CONFLICT) clause
        occupancy_change,  # param 10: for current_occupancy update
        occupancy_change,  # param 11: for free_spots update
        occupancy_change  # param 12: for occupancy_rate update
    ))

    # not commiting here
    print(f"SUCCESS (in batch): {event_type} event queued for {lot_id}.")

def lambda_handler(kinesis_event, context):
    # Kinesis stream processor function. Reads records and updates PostgreSQL
    logger.info("psycopg2 loaded from: %s", psycopg2.__file__)
    records_processed = 0
    failed_sequence_numbers = []  # For per-record retries
    conn = None
    
    try:
        conn = get_db_connection()

        # Batch Commit Logic:
        with conn.cursor() as cur:

            # process records from the Kinesis batch
            for record in kinesis_event['Records']:
                sequence_number = record['kinesis']['sequenceNumber']
                try:
                    # kinesis data is base64 encoded
                    payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                    event_data = json.loads(payload)

                    # Batch commit ---- pass cursor to processing function
                    process_event(event_data, cur)
                    records_processed += 1

                except Exception as e:
                    print(f"Error processing single record: {e} | Record: {record.get('kinesis', {}).get('data')}")
                    failed_sequence_numbers.append(sequence_number)

            # Commit the entire batch at once ---
            conn.commit()
            logger.info(f"Batch commit successful. Processed: {records_processed}, Failed: {len(failed_sequence_numbers)}")

    except EnvironmentError as e:
        logger.fatal(f"FATAL: Configuration error. {e}")
        print(f"FATAL: Configuration error. {e}")
        raise

    except Exception as e:
        logger.fatal(f"FATAL: Database or batch commit error: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise

    finally:
        # ensure connection is closed
        if conn:
            conn.close()

    print(f"Batch completed. Total records processed: {records_processed}")

    logger.info(f"Batch completed. Total records processed: {records_processed}")

    # Tell Kinesis which specific records to retry
    return {'batchItemFailures': [{'itemIdentifier': seq} for seq in failed_sequence_numbers]}
