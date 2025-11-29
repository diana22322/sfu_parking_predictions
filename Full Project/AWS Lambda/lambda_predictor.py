import json
import os
import boto3
import psycopg2
from datetime import datetime, timedelta, timezone
from psycopg2 import sql
from zoneinfo import ZoneInfo

# Configuration
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

ENDPOINT_OCCUPANCY = os.environ.get('ENDPOINT_OCCUPANCY', 'sfu-occupancy-predictor')
ENDPOINT_DEPARTURE = os.environ.get('ENDPOINT_DEPARTURE', 'sfu-departure-predictor')

# model feature columns - must match exactly as the training order
MODEL_FEATURES = ['occupancy_now', 'day_of_week', 'hour_of_day', 'is_holiday', 'is_weekend', 'is_exam_week',
    'lot_id_CENTRAL', 'lot_id_EAST', 'lot_id_NORTH', 'lot_id_SOUTH', 'lot_id_SRYC', 'lot_id_SRYE', 'lot_id_WEST',
    'campus_BURNABY', 'campus_SURREY']

# Initialize clients
sagemaker_runtime = boto3.client('sagemaker-runtime')

PST = ZoneInfo("America/Los_Angeles")



def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, connect_timeout=5
    )

def get_current_state(conn):
    # fetches the latest occupancy for all lots
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT lot_id, campus_name, current_occupancy, capacity FROM current_lot_occupancy
                    """)
        return cur.fetchall()
    
def invoke_endpoint(endpoint_name, payload):
    try:
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='text/csv',
            Body=payload
        )
        return response['Body'].read().decode()
    except Exception as e:
        print(f"Error invoking {endpoint_name}: {e}")
        return None
    
def prepare_features(lot_id, campus, current_occupancy):
    # constructs the feature vector for a single lot
    # using manual One-Hot Encoding here
    now = datetime.now(timezone.utc)

    # time features
    # Python day: 0=Mon, 6=Sun
    # Spark dayofweek: 1=Sun, 2=Mon,....
    # Spark was used in feature engineering so need to match Spark's dayofweek 
    # conversion: (isoweekday() % 7) + 1
    day_of_week = (now.isoweekday() % 7)  + 1
    hour_of_day = now.hour

    # calendar flags
    is_weekend = 1 if day_of_week in [1,7] else 0 # Sun=1, Sat=7
    is_holiday = 0
    is_exam_week = 0

    # construct feature dectionary
    features = {
        'occupancy_now': current_occupancy,
        'day_of_week': day_of_week,
        'hour_of_day': hour_of_day,
        'is_holiday': is_holiday,
        'is_weekend': is_weekend,
        'is_exam_week': is_exam_week,
        # Initialize all One-Hot columns to 0
        'lot_id_CENTRAL': 0, 'lot_id_EAST': 0, 'lot_id_NORTH': 0, 'lot_id_SOUTH': 0,
        'lot_id_SRYC': 0, 'lot_id_SRYE': 0, 'lot_id_WEST': 0,
        'campus_BURNABY': 0, 'campus_SURREY': 0
    }

    # set the active one-hot columns
    features[f'lot_id_{lot_id}'] = 1
    features[f'campus_{campus}'] = 1

    # convert to ordered csv string (no header)
    csv_values = [str(features[col]) for col in MODEL_FEATURES]
    return ",".join(csv_values)

def save_predictions(conn, predictions):
    # upserts predictions into the parking_forecasts table.
    sql_upsert = """
            INSERT INTO parking_forecast  
                (lot_id, forecast_time, run_time,
                predicted_occupancy_count,
                predicted_num_departures,
                predicted_occupancy_pct)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (lot_id, forecast_time) 
            DO UPDATE SET 
                run_time = EXCLUDED.run_time,
                predicted_occupancy_count = EXCLUDED.predicted_occupancy_count,
                predicted_num_departures = EXCLUDED.predicted_num_departures,
                predicted_occupancy_pct = EXCLUDED.predicted_occupancy_pct
            """
    
    sql_cleanup = """
            DELETE FROM parking_forecast AS p
            WHERE p.ctid IN (
                SELECT ctid
                FROM parking_forecast
                WHERE lot_id = p.lot_id
                ORDER BY run_time DESC
                OFFSET 5
            )
            """
    
    with conn.cursor() as cur:
        for p in predictions:
            cur.execute(sql_upsert, (
                p['lot_id'],
                p['target_time'],
                p['run_time'],
                p['pred_occ'],
                p['pred_dep'],
                p['pred_pct']
            ))

        cur.execute(sql_cleanup)
    conn.commit()

def lambda_handler(event, context):
    print('Starting Predictor Service ....')
    conn = None

    try:
        conn = get_db_connection()

        lots = get_current_state(conn)
        print(f"Fetched state for {len(lots)} lots")

        run_time_utc = datetime.now(timezone.utc)
        target_time_utc = run_time_utc + timedelta(minutes=15)

        run_time = run_time_utc.astimezone(PST)
        target_time = target_time_utc.astimezone(PST)

        # prepare all feature rows in order
        feature_rows = []
        lot_meta = []   # keep (lot_id, campus, capacity) in the same order

        for lot in lots:
            lot_id, campus, current_occ, capacity = lot
            payload_row = prepare_features(lot_id, campus, current_occ)
            feature_rows.append(payload_row)
            lot_meta.append((lot_id, campus, capacity))

        # single CSV payload (one row per lot)
        batch_payload = "\n".join(feature_rows)

        # call both endpoints once
        occ_raw = invoke_endpoint(ENDPOINT_OCCUPANCY, batch_payload)
        dep_raw = invoke_endpoint(ENDPOINT_DEPARTURE, batch_payload)

        occ_preds = [float(x) for x in occ_raw.strip().splitlines()]
        dep_preds = [float(x) for x in dep_raw.strip().splitlines()]

        print(f"RAW Departure Predictions: {dep_preds}")

        predictions = []
        for (lot_id, campus, capacity), occ, dep in zip(lot_meta, occ_preds, dep_preds):
            pred_occ = int(round(occ))
            pred_dep = int(round(dep))

            pred_occ = max(0, min(pred_occ, capacity))
            pred_dep = max(0, pred_dep)

            pred_pct = round(pred_occ / capacity, 2) if capacity > 0 else 0

            predictions.append({
                'lot_id': lot_id,
                'target_time': target_time,
                'run_time': run_time,
                'pred_occ': pred_occ,
                'pred_pct': pred_pct,
                'pred_dep': pred_dep
            })

        if predictions:
            save_predictions(conn, predictions)
            print(f"Successfully saved {len(predictions)} predictions.")

        return {'statusCode': 200, 'body': json.dumps('Prediction cycle complete!')}

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        return {'statusCode': 500, 'body': json.dumps('Prediction failed!')}
    finally:
        if conn:
            conn.close()