DESCRIPTION:
This folder contains the sever less functions used in the project. These Lambda functions handle real-time data ingestion, event stream processing, and updating the recent_events table in the DB. Together, they connect the real-time simulator output with the web application by maintaining up-to-date log of recent events. Lambda C consumers sagemaker endpoints on a scheduled time and writes prediction response to the DB.


FOLDER STRUCTURE:
AWS Lambda/
│
├── recent_events.py
├── psycopg2-layer.zip
├── lambda_ingester.py 		(Lambda A)
├── lambda_predictor.py  		(Lambda C)
└── lambda_kinesis_processor.py	(Lambda B)



CONTENTS:
· recent_events.py - maintains the recent_events PostgreSQL table used by the website. This lambda is responsible for inserting new arrival and departure events into the database; keeping only the latest 5 events to limit table size; formatting timestamps and normalizing structure. It is invoked by Kinesis processor after new events are ingested.
· lambda_ingester.py - processes raw parking events arriving into the system. It is responsible for validating the event payload structure; normalizing fields; writing the cleaned events; ensuring event stream remains consistent.
· lambda_kinesis_processor.py - triggered by Kinesis Data Stream batches. The function decodes incoming event records; extracts events in real time; updates current_lot_occupancy table in postgreSQL.
· psycopg2-layer.zip - python package that allows AWS Lambda to connect to postgreSQL RDS

. lambda_predictor.py: This function is triggered by the Eventbridge schedule to run every 15 minutes, it first fetches the latest occupancy state from DB, invokes Sagemaker endpoints, retrieves predictions for each lot, and writes results to DB (which is then consumed by the dashboard)

HOW IT WORKS:
lambda_ingester.py validates incoming parking events and sends them into the Kinesis stream after being triggered by API Gateway.
lambda_kinesis_processor.py and recent_events.py are triggered automatically by the Kinesis stream.

