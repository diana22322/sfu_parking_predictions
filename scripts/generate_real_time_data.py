import time
import random
import uuid
import requests
import json
from datetime import datetime, timedelta

API_GATEWAY_URL = "https://lyhqt4ywb0.execute-api.us-west-2.amazonaws.com/parking-event"
API_KEY  = ""

# parking lot capacities and weights
LOT_CONFIG = {
    'BURNABY': {
        'lots': ['NORTH', 'EAST', 'CENTRAL', 'WEST', 'SOUTH'],
        'capacity': {'NORTH': 2000, 'EAST': 1500, 'CENTRAL': 600, 'WEST': 500, 'SOUTH': 400},
        'traffic_weight': 0.90, # 90% of total events for this campus
        'min_duration_min': 90,
        'max_duration_min': 360,
    },
    'SURREY': {
        'lots': ['SRYC', 'SRYE'],
        'capacity': {'SRYC': 450, 'SRYE': 450},
        'traffic_weight': 0.10,
        'min_duration_min': 60,
        'max_duration_min': 240,
    }
}


# Create a flat dictionary to track occupancy for all lots
# e.g. {'NORTH': 0, 'EAST': 0, .....}
LOT_OCCUPANCY = {
    lot: 0
    for campus_cfg in LOT_CONFIG.values()
    for lot in campus_cfg['capacity'].keys()
}


# average delay between events (e.g. 10.0s = 1 event every 10 seconds)
EVENT_INTERVAL_SECONDS = 10.0

# define active window - generating events between 8am - 11:59pm
ACTIVE_HOUR_START = 8
ACTIVE_HOUR_END = 23

# peak arrival time - 8am - 11:59am
ARRIVAL_PEAK_HOUR_END = 11

# Unique IDs to simulate
STUDENT_COUNT = 18000
STUDENT_IDS = [f"{i+10000}" for i in range(STUDENT_COUNT)]

# map plate number for each student
def generate_plate():
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    digits = ''.join(random.choices('0123456789', k=3))
    suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1))
    return f"{letters}{digits}{suffix}"

PLATE_MAP = {sid: generate_plate() for sid in STUDENT_IDS}

# dictionary to keep track of active parking sessions
# key: student_id
# value: {{'session_id': UUID, 'lot_id': 'NORTH', 'campus': 'BURNABY', 'expected_departure': datetime, 'plate_number': '...' }
ACTIVE_SESSIONS = {}

def generate_arrival_event():
    campus_name = random.choices(list(LOT_CONFIG.keys()),
                                 weights=[c['traffic_weight'] for c in LOT_CONFIG.values()],
                                 k=1)[0]
    config = LOT_CONFIG[campus_name]

    # try preferred lot
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
        print(f"[{datetime.now().strftime('%H:%M:%S')}] REJECTED | CAMPUS: {campus_name} | "
              f"All lots full. | ACTIVE Sessions: {len(ACTIVE_SESSIONS)}")
        return None

    # spot found -> proceed with event generation
    # select student (ensure that they are not currently parked)
    available_students = [sid for sid in STUDENT_IDS if sid not in ACTIVE_SESSIONS]
    if not available_students:
        return None

    student_id = random.choice(available_students)
    plate_number = PLATE_MAP[student_id]

    # determine parking duration
    duration = random.randint(config['min_duration_min'], config['max_duration_min'])
    event_time = datetime.now()
    departure_time = event_time + timedelta(minutes=duration)
    session_id = str(uuid.uuid4())

    # update occupancy
    LOT_OCCUPANCY[assigned_lot] += 1

    # store session in active sessions -- needed for departure event
    ACTIVE_SESSIONS[student_id] = {
        'session_id': session_id,
        'lot_id': assigned_lot,
        'campus': campus_name,
        'expected_departure': departure_time,
        'plate_number': plate_number
    }

    return {
        "session_id": session_id,
        "timestamp": event_time.isoformat() + 'Z',
        "event_type": "ARRIVAL",
        "lot_id": assigned_lot,
        "campus": campus_name,
        "student_id": student_id,
        "plate_number": plate_number,
    }

def generate_departure_event():
    """generates a departure event for a session show time has passed"""

    # find eligible departure sessions
    eligibile_departures = []
    current_time = datetime.now()

    for student_id, session in ACTIVE_SESSIONS.items():
        if session['expected_departure'] <= current_time:
            eligibile_departures.append((student_id, session))

    if not eligibile_departures:
        return None

    # Select a session to depart -- prioritize longest overdue
    eligibile_departures.sort(key=lambda x: x[1]['expected_departure'])
    student_id, session = eligibile_departures[0]
    plate_number = session['plate_number']

    departing_lot = session['lot_id']

    LOT_OCCUPANCY[departing_lot] -= 1
    if LOT_OCCUPANCY[departing_lot] < 0:
        LOT_OCCUPANCY[departing_lot] = 0

    # remove from active sessions
    del ACTIVE_SESSIONS[student_id]

    return {
        "session_id": session['session_id'],
        "timestamp": current_time.isoformat() + 'Z',
        "event_type": "DEPARTURE",
        "lot_id": session['lot_id'],
        "campus": session['campus'],
        "student_id": student_id,
        "plate_number": plate_number
    }

def send_event_to_api(event_payload):
    # send event to API gateway endpoint
    if not event_payload:
        return False

    headers = {
        'Content-type': 'application/json',
        'x-api-key': API_KEY # maybe useful later -- if using API key
    }

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

def run_simulation():
    print(f"---SFU Real_time Parking Simulator Started ----")
    print(f"API target: {API_GATEWAY_URL}")
    print(f"Event Interval: {EVENT_INTERVAL_SECONDS}s")
    print(f"Active Window: {ACTIVE_HOUR_START:02d}:00 to {ACTIVE_HOUR_END:02d}:59")
    print(f"Total Student IDs: {len(STUDENT_IDS)}")
    print(f"Initial Occupancy: {LOT_OCCUPANCY}")
    print("------------------------------------------------")

    # check if API URL is set
    if "YOUR_API_GATEWAY_ID" in API_GATEWAY_URL:
        print("\nERROR: Please update API_GATEWAY_URL and API_KEY in the script configuration.")
        return

    while True:
        current_time = datetime.now()
        current_hour = current_time.hour

        # enforce active window
        if ACTIVE_HOUR_START <= current_hour <= ACTIVE_HOUR_END:
            # generate arrival or departure
            departure_chance = 0.5
            can_depart = any(s['expected_departure'] <= current_time for s in ACTIVE_SESSIONS.values())

            if current_hour <= ARRIVAL_PEAK_HOUR_END:
                departure_chance = 0.4
            elif current_hour > ARRIVAL_PEAK_HOUR_END:
                departure_chance = 0.7

            if can_depart and random.random() < departure_chance:
                event = generate_departure_event()
            else:
                event = generate_arrival_event()

            send_event_to_api(event)

            # use short time interval for event generation
            time.sleep(EVENT_INTERVAL_SECONDS)
        else:
            # off-hours sleep
            # calculate time until 8am
            print(f"[{current_time.strftime('%H:%M:%S')}] OFF-HOURS. Pausing event generation....")

            next_active_time = current_time.replace(hour=ACTIVE_HOUR_START, minute=0, second=0, microsecond=0)

            # If current time is past 11:59pm, jump to 8am the next day
            if current_hour > ACTIVE_HOUR_END:
                next_active_time += timedelta(days=1)

            sleep_duration = (next_active_time - current_time).total_seconds()

            # cap the sleep to 1 hour max to ensure log output
            if sleep_duration > 3600:
                sleep_duration = 3600

            time.sleep(sleep_duration)

if __name__ == '__main__':
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("Simulation stopped by user")
        print(f"Final Active Sessions: {len(ACTIVE_SESSIONS)}")
        print(f"Final Lot Occupancy: {LOT_OCCUPANCY}")