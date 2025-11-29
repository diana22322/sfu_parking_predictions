import time
import random
import uuid
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dtime

# Configuration
API_GATEWAY_URL = "https://lyhqt4ywb0.execute-api.us-west-2.amazonaws.com/parking-event"
API_KEY  = ""

# lot capacities
LOT_CONFIG = {
    'BURNABY': {
        'lots': ['NORTH', 'EAST', 'CENTRAL', 'WEST', 'SOUTH'],
        'capacity': {'NORTH': 2000, 'EAST': 1500, 'CENTRAL': 600, 'WEST': 500, 'SOUTH': 400},
        'traffic_weight': 0.90,
    },
    'SURREY': {
        'lots': ['SRYC', 'SRYE'],
        'capacity': {'SRYC': 450, 'SRYE': 450},
        'traffic_weight': 0.10,
    }
}

# simulation settings
STUDENT_COUNT = 10000 # active student IDs to simulate daily parking
STUDENT_IDS = [f"{i+10000}" for i in range(STUDENT_COUNT)]
BATCH_SIZE = 50 # number of events to process per loop iteration
LOOP_INTERVAL_SECONDS = 5.0 # wait time between sending batches

# define the common class times based on SFU schedule
# classes start at :30 and end at :20
CLASS_START_TIMES = [dtime(h, 30) for h in range(8, 18)] # 8:30 to 17:30
CLASS_END_TIMES = [dtime(h, 20) for h in range(9, 19)] # 9:20 to 18:20 

LOT_OCCUPANCY = {
    lot: 0
    for campus_cfg in LOT_CONFIG.values()
    for lot in campus_cfg['capacity'].keys()
}

# dictionary to keep track of active parking sessions
# key: student_id
# value: {'session_id': UUID, 'lot_id': 'NORTH', 'campus': 'BURNABY', 'plate_number': '...' }
ACTIVE_SESSIONS = {}

# Queue of pre-generated and scheduled events for the current day
DAILY_SCHEDULE_QUEUE = []

# map plate number for each student
def generate_plate():
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    digits = ''.join(random.choices('0123456789', k=3))
    suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1))
    return f"{letters}{digits}{suffix}"

PLATE_MAP = {sid: generate_plate() for sid in STUDENT_IDS}

# Schedule generate - aligning with the historical schedule
def get_daily_schedule():
    """
    Generates a synthetic schedule of ARRIVAL and DEPARTURE events for a single day.
    """
    print(f"Generating new daily schedule for {datetime.now().strftime('%Y-%m-%d')}...")

    # define campus assignments for students 
    burnaby_max = int(STUDENT_COUNT * 0.90)
    burnaby_students = random.sample(STUDENT_IDS, burnaby_max)
    surrey_students = [sid for sid in STUDENT_IDS if sid not in burnaby_students]

    campus_map = {sid: 'BURNABY' for sid in burnaby_students}
    campus_map.update({sid: 'SURREY' for sid in surrey_students})
    active_students = burnaby_students + surrey_students

    daily_events = []
    is_weekend = datetime.now().weekday() >= 5
    
    if is_weekend:
        # weekend -> low demand
        print("Weekend detected. Generating reduced, non-class schedule.")
        
        # only simulate 10% of students arriving on weekends
        active_weekend_students = random.sample(active_students, int(len(active_students) * 0.10))
        
        for student_id in active_weekend_students:
            campus = campus_map[student_id]
            session_id = str(uuid.uuid4())
            
            # simulate arrivals between 9:00 AM and 12:00 PM
            start_hour = random.randint(9, 11)
            start_minute = random.randint(0, 59)
            arrival_time = datetime.now().replace(hour=start_hour, minute=start_minute, second=0)

            # simulate parking duration of 2 to 5 hours
            duration_minutes = random.randint(120, 300) 
            departure_time = arrival_time + timedelta(minutes=duration_minutes)
            
            # create ARRIVAL Event
            daily_events.append({
                "session_id": session_id,
                "timestamp": arrival_time,
                "event_type": "ARRIVAL",
                "student_id": student_id,
                "campus": campus,
                "plate_number": PLATE_MAP[student_id],
            })

            # create DEPARTURE Event
            daily_events.append({
                "session_id": session_id,
                "timestamp": departure_time,
                "event_type": "DEPARTURE",
                "student_id": student_id,
                "campus": campus,
                "plate_number": PLATE_MAP[student_id],
            })

    else:
        # weekdays -> high demand, class-driven
        for student_id in active_students:
            # sssign 1 to 3 classes per day
            num_classes = np.random.randint(1, 4)

            # randomly assign unique class start times
            selected_starts = np.random.choice(CLASS_START_TIMES, size=num_classes, replace=False)
            student_classes = []

            for start_time_obj in selected_starts:
                # randomly assign duration: 1 hour (60%) or 2 hours (40%)
                duration = int(np.random.choice([1, 2], p=[0.6, 0.4]))
                
                # find the corresponding end time based on the class structure
                if duration == 1:
                    end_hour = start_time_obj.hour + 1
                else: # duration: 2
                    end_hour = start_time_obj.hour + 2

                # cap end time at 18:20
                if end_hour > 18:
                    end_hour = 18 

                end_time_obj = dtime(end_hour, 20)

                student_classes.append({
                    'start_time': start_time_obj,
                    'end_time': end_time_obj
                })

            if not student_classes:
                continue

            # calculate Arrival/Departure times
            # first class start time
            first_start = min(c['start_time'] for c in student_classes)
            # last class end time
            last_end = max(c['end_time'] for c in student_classes)
            
            # Arrival: 15 to 45 minutes before first class start 
            arrival_buffer = np.random.uniform(15, 45)
            arrival_time = (datetime.now().replace(hour=first_start.hour, minute=first_start.minute, second=0) 
                            - timedelta(minutes=arrival_buffer))
            
            # Departure: 15 to 45 minutes after last class ends
            departure_buffer = np.random.uniform(15, 45)
            departure_time = (datetime.now().replace(hour=last_end.hour, minute=last_end.minute, second=0) 
                              + timedelta(minutes=departure_buffer))

            # truncate to the end of the current day (23:59:59)
            today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
            departure_time = min(departure_time, today_end)


            # create events
            campus = campus_map[student_id]
            session_id = str(uuid.uuid4())

            # Arrival event
            daily_events.append({
                "session_id": session_id,
                "timestamp": arrival_time,
                "event_type": "ARRIVAL",
                "student_id": student_id,
                "campus": campus,
                "plate_number": PLATE_MAP[student_id],
            })

            # Departure event
            daily_events.append({
                "session_id": session_id,
                "timestamp": departure_time,
                "event_type": "DEPARTURE",
                "student_id": student_id,
                "campus": campus,
                "plate_number": PLATE_MAP[student_id],
            })


    # sort all events chronologically
    daily_events.sort(key=lambda x: x['timestamp'])
    print(f"Schedule generated with {len(daily_events)} events.")
    return daily_events

# Event processing
def generate_arrival_event(event_data):
    """
    Tries to assign a lot based on real-time occupancy. Returns event payload if successful.
    """
    campus_name = event_data['campus']
    student_id = event_data['student_id']
    config = LOT_CONFIG[campus_name]

    # try preferred lot (randomly chosen preference for simplicity)
    preferred_lot = random.choice(config['lots'])
    assigned_lot = None

    # check preferred lot first
    if LOT_OCCUPANCY[preferred_lot] < config['capacity'][preferred_lot]:
        assigned_lot = preferred_lot
    else:
        # preferred lot is full, try other lots on the same campus
        other_lots = [lot for lot in config['lots'] if lot != preferred_lot]
        random.shuffle(other_lots)

        for lot in other_lots:
            if LOT_OCCUPANCY[lot] < config['capacity'][lot]:
                assigned_lot = lot
                break # found a spot

    if assigned_lot is None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] REJECTED ARRIVAL | CAMPUS: {campus_name} | All lots full.")
        return None

    # spot found -> proceed with event generation
    LOT_OCCUPANCY[assigned_lot] += 1

    # store session in active sessions -- needed for departure event
    # only store minimal info for lot assignment logic
    ACTIVE_SESSIONS[student_id] = {
        'session_id': event_data['session_id'],
        'lot_id': assigned_lot,
        'campus': campus_name,
        'plate_number': event_data['plate_number']
    }

    return {
        "session_id": event_data['session_id'],
        "timestamp": event_data['timestamp'].isoformat() + 'Z',
        "event_type": "ARRIVAL",
        "lot_id": assigned_lot,
        "campus": campus_name,
        "student_id": student_id,
        "plate_number": event_data['plate_number'],
    }


def generate_departure_event(event_data):
    """
    Generates a departure event if the student is actively parked.
    """
    student_id = event_data['student_id']

    if student_id not in ACTIVE_SESSIONS:
        # student was never parked (e.g., arrival was rejected due to full lot)
        return None

    session = ACTIVE_SESSIONS[student_id]
    departing_lot = session['lot_id']

    # update occupancy
    LOT_OCCUPANCY[departing_lot] -= 1
    if LOT_OCCUPANCY[departing_lot] < 0:
        LOT_OCCUPANCY[departing_lot] = 0

    # remove from active sessions
    del ACTIVE_SESSIONS[student_id]

    return {
        "session_id": session['session_id'],
        "timestamp": datetime.now().isoformat() + 'Z', # Use current time for departure report
        "event_type": "DEPARTURE",
        "lot_id": session['lot_id'],
        "campus": session['campus'],
        "student_id": student_id,
        "plate_number": session['plate_number']
    }


def send_event_to_api(event_payload):
    # dummy function for a realistic simulation - will only print to console
    if not event_payload:
        return False
    
    headers = {'Content-type': 'application/json', 'x-api-key': API_KEY}

    try:
        response = requests.post(API_GATEWAY_URL, headers=headers, data=json.dumps(event_payload))
        response.raise_for_status() # raise HTTPError for bad response

        event_type = event_payload['event_type']
        lot_id = event_payload['lot_id']
        campus = event_payload['campus']
        session_count = len(ACTIVE_SESSIONS)
        lot_cap = LOT_CONFIG[campus]['capacity'][lot_id]
        lot_occ = LOT_OCCUPANCY[lot_id]

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] SENT {event_type} | Lot: {lot_id} ({lot_occ}/{lot_cap}) | "
            f"Plate: {event_payload.get('plate_number')} | Active Sessions: {session_count}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] FAILED to send event to API Gateway: {e}")
        print(f"Payload: {event_payload}")

        # rollback occupancy on failure
        # un-park them, if the arrival event failed
        if event_payload['event_type'] == 'ARRIVAL':
            student_id = event_payload['student_id']
            if student_id in ACTIVE_SESSIONS:
                # remove from active session
                del ACTIVE_SESSIONS[student_id]
                # decrement occupancy
                lot_id = event_payload['lot_id']
                LOT_OCCUPANCY[lot_id] -= 1
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ROLLBACK: Removed {student_id} from {lot_id} due to API failure.")

        # if departure failed
        elif event_payload['event_type'] == 'DEPARTURE':
            # just logging the error as main risk is ARRIVAL failue
            pass

        return False
# Simulation loop
def run_simulation():
    global DAILY_SCHEDULE_QUEUE

    print(f"--- SFU Real-time Parking Simulator (Schedule-Driven) Started ----")
    print(f"Batch Size: {BATCH_SIZE} events every {LOOP_INTERVAL_SECONDS}s")
    print(f"Class times: Start :30, End :20 | Peak Hours: 10:30, 11:30, 12:30, 1:30pm, 2:30pm")
    print("------------------------------------------------------------------")

    # check if API URL is set
    if "YOUR_API_GATEWAY_ID" in API_GATEWAY_URL:
        print("\nNOTE: Please update API_GATEWAY_URL and API_KEY if you want to send real data.")

    last_schedule_date = None

    while True:
        current_time = datetime.now()
        current_date = current_time.date()
        
        # generate/refresh Schedule at the start of a new day
        if current_date != last_schedule_date:
            DAILY_SCHEDULE_QUEUE = get_daily_schedule()
            last_schedule_date = current_date
            
            # reset active sessions and occupancy at midnight
            ACTIVE_SESSIONS.clear()
            for lot in LOT_OCCUPANCY:
                 LOT_OCCUPANCY[lot] = 0
            
            # If no schedule, wait a bit
            if not DAILY_SCHEDULE_QUEUE:
                 print("No events generated for today. Sleeping for 1 hour...")
                 time.sleep(3600)
                 continue

        
        # process DUE events in batches
        due_events = [
            event for event in DAILY_SCHEDULE_QUEUE
            if event['timestamp'] <= current_time
        ]
        
        events_to_process = due_events[:BATCH_SIZE]
        
        if events_to_process:
            # print(f"\nProcessing batch of {len(events_to_process)} events...")
            
            # remove processed events from the queue
            DAILY_SCHEDULE_QUEUE = DAILY_SCHEDULE_QUEUE[len(events_to_process):]
            
            for event in events_to_process:
                event_payload = None
                
                if event['event_type'] == 'ARRIVAL':
                    event_payload = generate_arrival_event(event)
                elif event['event_type'] == 'DEPARTURE':
                    event_payload = generate_departure_event(event)
                
                send_event_to_api(event_payload)

        # wait for the next batch
        time.sleep(LOOP_INTERVAL_SECONDS)


if __name__ == '__main__':
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("\nSimulation stopped by user")
        print(f"Final Active Sessions: {len(ACTIVE_SESSIONS)}")
        print(f"Final Lot Occupancy: {LOT_OCCUPANCY}")