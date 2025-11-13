import json
import os
import boto3
from datetime import datetime, timedelta, timezone

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- CONFIGURATION (ENVIRONMENT VARIABLES) ---
# This is read from the Lambda's environment variables
KINESIS_STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME', 'sfu-parking-stream')

# Validation constants (must match PySpark ETL and Simulator)
VALID_CAMPUSES = {'BURNABY', 'SURREY'}
VALID_EVENT_TYPES = {'ARRIVAL', 'DEPARTURE'} 
VALID_LOTS = {
    'BURNABY': {'NORTH', 'EAST', 'CENTRAL', 'WEST', 'SOUTH'},
    'SURREY': {'SRYC', 'SRYE'}
}

REQUIRED_FIELDS = ['session_id', 'timestamp', 'event_type', 'lot_id', 'campus', 'student_id', 'plate_number']


def validate_event(event: dict) -> dict:
    """Performs mandatory checks and standardization on the incoming event payload."""
    # check that all fields are present
    for field in REQUIRED_FIELDS:
        if field not in event or not event[field]:
            raise ValueError(f"Missing or empty required field: {field}")

    # statndarize, convert to uppercase to match Kinesis processor and ETL consistency
    event['campus'] = str(event['campus']).upper().strip()
    event['lot_id'] = str(event['lot_id']).upper().strip()
    event['event_type'] = str(event['event_type']).upper().strip()

    event['student_id'] = str(event['student_id'])

    # format checks
    if event['event_type'] not in VALID_EVENT_TYPES:
        raise ValueError(f"Invalid event_type: {event['event_type']}")

    if event['campus'] not in VALID_CAMPUSES:
        raise ValueError(f"Invalid campus: {event['campus']}")

    if event['lot_id'] not in VALID_LOTS.get(event['campus'], set()):
        raise ValueError(f"Lot ID {event['lot_id']} is not valid for campus {event['campus']}")

    # validate timestamp
    try:
        # parse the incoming timestamp (it should be UTC)
        event_ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))

        # get the current UTC time, ensuring includes timezone
        current_utc = datetime.now(timezone.utc)

        # Check against current UTC time plus a small buffer (1 minute)
        if event_ts > current_utc + timedelta(minutes=1):
            raise ValueError('Timestamp is in the future')

    except ValueError:
        raise ValueError('Invalid Timestamp format or future timestamp check failed.')

    return event


# Global variable for Kinesis client (will be initialized on first run)
KINESIS_CLIENT = None

def lambda_handler(api_event, context):
    """Receives event from API Gateway, validates it, and sends it to Kinesis."""
    logger.info("HANDLER ENTER")
    logger.info("RAW EVENT: %s", api_event)
    global KINESIS_CLIENT

    # Initialize Boto3 Kinesis client inside the handler (or a wrapper)
    # to ensure ENI is active before initialization.
    if KINESIS_CLIENT is None:
        try:
            KINESIS_CLIENT = boto3.client('kinesis')
        except Exception as e:
            # If Boto3 fails to initialize (e.g., DNS resolution issue), fail early
            print(f"FATAL ERROR: Failed to initialize Kinesis client: {e}")
            raise

    try:
        # API gateway sends the actual request body as a string inside 'body' key
        body_string = api_event.get('body', '')
        if not body_string:
            raise ValueError('Empty request body')
        body = json.loads(body_string)
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON body format.'})
        }

    try:
        # Validate and standardize the event
        validated_event = validate_event(body)

        logger.info("ABOUT TO PUT TO KINESIS: %s", validated_event)

        # Send to Kinesis using lot_id as the Partition Key
        response = KINESIS_CLIENT.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(validated_event).encode('utf-8'),
            PartitionKey=validated_event['lot_id']
        )

        # Success logging
        logger.info("SUCCESS: %s", response)
        print(
            f"INFO: Successfully sent {validated_event['event_type']} event to Kinesis. ShardId: {response.get('ShardId')}")

        # Return success to API Gateway
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event successfully received and sent to Kinesis.',
                'shardId': response.get('ShardId')
            })
        }

    except ValueError as e:
        # Handle validation error gracefully
        print(f"Validation Failed: {e} | Raw event: {body}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f'Validation Error: {str(e)}'})
        }

    except Exception as e:
        # Handle unexpected errors e.g. Kinesis connection issues
        print(f"Kinesis Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal Server Error during stream write.'})
        }
