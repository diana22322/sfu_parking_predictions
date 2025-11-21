import numpy as np
import pandas as pd
import os
import sys
import uuid
from datetime import timedelta, date, time

"""
Synthetic Historical Parking Data Generator
-------------------------------------------
Generates synthetic parking data for the Burnaby and Surrey campuses, covering Fall and Spring
semesters from Fall 2023 to the present.
The data is designed to approximate realistic parking patterns and occupancy trends,
based on typical campus usage behavior and domain knowledge.
"""

OUTPUT_DIR = 'raw/historical'
STUDENT_COUNT = 20000       # Unique student ID's to simulate

# Define today's date (the point where data generation must stop)
TODAY = date(2025, 11, 20)
OFF_DAY_FRACTION = 0.12 # ~12% of activate students show up on the weekends too

# List of stat holidays affecting campus activities/parking
STAT_HOLIDAYS = [
    '2022-02-21', '2022-04-15',
    '2022-10-10', '2022-11-11',
    '2023-10-09', '2023-11-13',
    '2024-02-12', '2024-03-29',
    '2024-10-14', '2024-11-11',
    '2025-02-17', '2025-04-18',
    '2025-10-13', '2025-11-11' # Tentative Fall 2025 Holidays
]
# Converting to a set for fast lookup
HOLIDAYS_DT = {pd.to_datetime(d).date() for d in STAT_HOLIDAYS}

# Dates for major, non-class events that affect parking (e.g. Convocation)
SPECIAL_DAYS = [
    '2022-06-07', '2022-06-10',
    '2022-10-06', '2022-10-07',
    '2023-10-05', '2023-10-06',
    '2024-06-11', '2024-06-14',
    '2024-10-03', '2024-10-04',
    '2025-06-10', '2025-06-13',
    '2025-10-02', '2025-10-03'
]

SPECIAL_DAYS_DT = {pd.to_datetime(d).date() for d in SPECIAL_DAYS}

BURNABY_LOTS = {
    'North' : 2000, 'East' : 1500, 'Central' : 600, 'West' : 500, 'South' : 400
}
BURNABY_WEIGHTS = [0.35, 0.30, 0.15, 0.10, 0.10]

SURREY_LOTS = {
    'SRYC' : 450,  # Central City - Level P3
    'SRYE' : 450   # Underground Parkade
}
# Surrey lot preference (SRYC likely gets more traffic)
SURREY_WEIGHTS = [0.60, 0.40]

# --- COMBINED LOT AND CAMPUS DATA STRUCTURE ---
CAMPUS_LOTS = {
    'Burnaby': {'lots': BURNABY_LOTS, 'weights': BURNABY_WEIGHTS, 'max_students': int(STUDENT_COUNT * 0.90)},
    'Surrey': {'lots': SURREY_LOTS, 'weights': SURREY_WEIGHTS, 'max_students': int(STUDENT_COUNT * 0.10)} # Assume ~10% of students primarily use Surrey
}

# extract names for simpler access later
BURNABY_LOT_NAMES = list(BURNABY_LOTS.keys())
SURREY_LOT_NAMES = list(SURREY_LOTS.keys())


# Semester definitions (start, end, exam boundaries)
# two-week exam period at the end of the semester
SEMESTERS = [
    {'name': 'Spring 2022', 'start': '2022-01-10', 'class_end': '2022-04-11', 'sem_end': '2022-04-26'},
    {'name': 'Fall 2022', 'start': '2022-09-07', 'class_end': '2022-12-06', 'sem_end': '2022-12-19'},
    {'name': 'Spring 2023', 'start': '2023-01-04', 'class_end': '2023-04-11', 'sem_end': '2023-04-24'},
    {'name': 'Fall 2023', 'start': '2023-09-05', 'class_end': '2023-12-08', 'sem_end': '2023-12-22'},
    {'name': 'Spring 2024', 'start': '2024-01-08', 'class_end': '2024-04-12', 'sem_end': '2024-04-26'},
    {'name': 'Fall 2024', 'start': '2024-09-03', 'class_end': '2024-12-06', 'sem_end': '2024-12-20'},
    {'name': 'Spring 2025', 'start': '2025-01-06', 'class_end': '2025-04-11', 'sem_end': '2025-04-25'},
    {'name': 'Fall 2025', 'start': '2025-09-02', 'class_end': '2025-12-05', 'sem_end': '2025-12-19'},
]

# ------------ Generating synthetic schedules for students -----------------------
def generate_synthetic_schedule(start_date, end_date):
    """
        Generates a simplified, realistic course schedule template for the entire period
        This drives the core demand signal
    """
    # define common class times
    class_times = [time(h, m) for h in range(8,17) for m in [30]]

    # sample active students and assign a primary campus
    student_ids = pd.Series(range(10000, STUDENT_COUNT + 10000))

    # active students for burnaby campus ~85%
    burnaby_students = student_ids.sample(n=CAMPUS_LOTS['Burnaby']['max_students'], replace=False).tolist()

    # active students for surrey campus ~15%
    surrey_students = student_ids[~student_ids.isin(burnaby_students)]\
        .sample(n=CAMPUS_LOTS['Surrey']['max_students'], replace=False).tolist()

    # create mapping of students to campus
    campus_map = {sid: 'Burnaby' for sid in burnaby_students}
    campus_map.update({sid: 'Surrey' for sid in surrey_students})
    active_students = surrey_students + burnaby_students

    schedule_data = []

    for student_id in active_students:
        # assigning 1 to 3 classes per day to these students
        num_classes = np.random.randint(1, 4)

        # Randomly assign class days and times
        days = np.random.choice(['M', 'T', 'W', 'Th', 'F'], size=num_classes, replace=False)
        times = np.random.choice(class_times, size=num_classes, replace=False)

        for day, time_obj in zip(days, times):
            # determine a random duration: 1 (40%) or 2 hour (60%) long class
            duration = int(np.random.choice([1,2], p=[0.6, 0.4]))

            start = pd.to_datetime(str(time_obj))
            end = start + timedelta(hours=duration)

            schedule_data.append({
                'student_id': student_id,
                'campus': campus_map[student_id],
                'day': day,
                'start_time': time_obj,
                'end_time': end.time()
            })

    # dataframe consisting of the weekly class schedule template
    schedule_df = pd.DataFrame(schedule_data)

    # Create a date sequence (calendar) for the entire schedule to iterate over and create daily schedules from the above weekly template
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # list to store dataframes consisting of daily schedules
    all_sessions = []

    for date_item in date_range:
        # get first letters of the day ['M', 'T', 'W' ...] from the 3-letter abbrev ['Mon', 'Tue',....]
        # wll use to match the 'day' column in schedule_df
        day_char = date_item.strftime('%a')[0]

        # only process weekdays for class schedules
        if date_item.weekday() < 5:
            # filter base the schedule to current weekday and add the date
            daily_schedule = schedule_df[schedule_df['day'].str.contains(day_char, na=False)].copy()
            daily_schedule['date'] = date_item.date()
            all_sessions.append(daily_schedule)

    # Concatenate all daily schedules to one larger df
    return pd.concat(all_sessions, ignore_index=True)

# ------------- Parking Event Simulation --------------------------
def simulate_events(schedule_df, current_period, student_frac=1.0):
    """
    Create ARRIVAL and DEPARTURE events based on the schedule and period logic
    """

    events = []

    is_busy_period = current_period == 'busy'
    is_exam_period = current_period == 'exam'
    is_off_day = current_period == 'off_day'

    # Define the observed worst peak window for arrival (10:30 AM to 12:30 PM)
    PEAK_START = time(10, 30)
    PEAK_END = time(12, 30)

    #*****************
    # Reduce sample of students for low-demand periods (used for off_day). Only subset of students come to campus on weekends on holidays
    if student_frac < 1.0:
        active_students = schedule_df['student_id'].unique()
        if len(active_students) > 0:
            sample_size = max(1, int(len(active_students) * student_frac))
            schedule_df = schedule_df[schedule_df['student_id'].isin(
                np.random.choice(active_students, size=sample_size, replace=False)
            )]
        else:
            return pd.DataFrame(events)

    # find the earliest class and last class for each student on this day
    # to anticipate their arrival and departure time
    daily_summary = schedule_df.groupby(['student_id', 'date', 'campus']).agg(
        first_start =('start_time', 'min'),
        last_end =('end_time', 'max')
    ).reset_index()


    # Add calendar flags for later data analysis
    daily_summary['isHoliday'] = daily_summary['date'].apply(lambda d: d in HOLIDAYS_DT)
    daily_summary['isWeekend'] = daily_summary['date'].apply(lambda d: d.weekday() >= 5)
    daily_summary['isExamWeek'] = is_exam_period
    daily_summary['isFirst2Weeks'] = is_busy_period
    daily_summary['isSpecialDay'] = daily_summary['date'].apply(lambda d: d in SPECIAL_DAYS_DT)

    # Iterate over the daily schedule of each student and generate ARRIVAL and DEPARTURE events for them
    for _, row in daily_summary.iterrows():
        # skip if date is after today
        if row['date'] > TODAY:
            continue

        # generate a unique session id: required for linking ARRIVAL/DEPARTURE
        session_id = str(uuid.uuid4())

        # arrival time calculation
        arrival_buffer = np.random.uniform(15, 45) # Base buffer for arrival before class start
        start_time_p = row['first_start'] # default anchor point

        # adjust buffer for special days
        if row['isSpecialDay']:
            arrival_buffer = np.random.uniform(1, 20) # to represent very tight scramble for busy days
        elif is_off_day:
            # simulate students arriving between 9am to 11:30am during holidays (no class schedule to anchor to)
            start_time_p = time(8, 30)
            arrival_buffer = np.random.uniform(30, 180)
        else:
            # regular days
            if is_busy_period or (PEAK_START <= start_time_p <= PEAK_END):
                arrival_buffer = np.random.uniform(5, 30) # peak/busy time
            elif is_exam_period:
                arrival_buffer = np.random.uniform(10, 60)

        # calculate arrival time
        start_dt = pd.to_datetime(str(row['date']) + ' ' + str(start_time_p))
        arrival_time = start_dt - timedelta(minutes=arrival_buffer)

        # ---- departure time calculation ----
        departure_buffer = np.random.uniform(15, 180) # default 15 min -3 hours after last class ends
        end_dt = pd.to_datetime(str(row['date']) + ' ' + str(row['last_end'])) # default end anchor

        # adjust buffer
        if is_off_day:
            # Predict a long study session relative to arrival time
            end_dt = arrival_time
            departure_buffer = np.random.uniform(120, 300)

        departure_time = end_dt + timedelta(minutes=departure_buffer)

        # if departure is scheduled for future, truncate it to end of TODAY
        if departure_time.date() > TODAY:
            departure_time_cap = pd.to_datetime(str(TODAY) + '23:59:59')
        else:
            departure_time_cap = departure_time

        current_campus = row['campus']

        # append arrival event
        events.append({
            'session_id': session_id,
            'timestamp': arrival_time,
            'event_type': 'ARRIVAL',
            'student_id': row['student_id'],
            'campus': current_campus,
            'isHoliday': row['isHoliday'],
            'isWeekend': row['isWeekend'],
            'isExamWeek': row['isExamWeek'],
            'isFirst2Weeks': row['isFirst2Weeks'],
            'isSpecialDay': row['isSpecialDay'],
        })

        # append departure event
        if departure_time_cap.date() <= TODAY:
            events.append({
                'session_id': session_id,
                'timestamp': departure_time_cap,
                'event_type': 'DEPARTURE',
                'student_id': row['student_id'],
                'campus': current_campus,
                'isHoliday': row['isHoliday'],
                'isWeekend': row['isWeekend'],
                'isExamWeek': row['isExamWeek'],
                'isFirst2Weeks': row['isFirst2Weeks'],
                'isSpecialDay': row['isSpecialDay'],
            })

    return pd.DataFrame(events)


def assign_lots_with_capacity(events_df):
    """
    processes a chronologically sorted df of events and assigns lots based on
    real-time availability
    """
    print("Assigning lots with capacity simulation...")

    # initialize capacity and trackers
    all_lot_capacities = {**BURNABY_LOTS, **SURREY_LOTS}
    lot_occupancy = {lot: 0 for lot in all_lot_capacities.keys()}

    # stores {session_id: assigned_lot} for active parkers
    active_sessions = {}

    # tracks sessions that failed to park (all lots full)
    rejected_session_ids = set()

    # store the new processed rows
    processed_events = []

    # iterate through all events
    for row in events_df.itertuples(index=False):
        session_id = row.session_id
        event_type = row.event_type
        campus = row.campus

        # if this session was already rejected, skip its departure too
        if session_id in rejected_session_ids:
            continue

        if event_type == 'DEPARTURE':
            # get the lot this session was in
            assigned_lot = active_sessions.pop(session_id, None)

            if assigned_lot:
                # free up the spot in this lot
                lot_occupancy[assigned_lot] -= 1
                if lot_occupancy[assigned_lot] < 0:
                    lot_occupancy[assigned_lot] = 0

                # add lot_id to event and store it
                event_data = row._asdict()
                event_data['lot_id'] = assigned_lot
                processed_events.append(event_data)
            # else - this departure has no matching arrival (it was rejected), so we just drop it

        elif event_type == 'ARRIVAL':
            # get campus specific lot info
            if campus == 'Burnaby':
                campus_lots = BURNABY_LOT_NAMES
                campus_weights = BURNABY_WEIGHTS
            elif campus == 'Surrey':
                campus_lots = SURREY_LOT_NAMES
                campus_weights = SURREY_WEIGHTS
            else:
                continue # unknown campus

            # try to find a lot
            preferred_lot = np.random.choice(campus_lots, p=campus_weights)
            assigned_lot = None

            # try preferred lot first
            if lot_occupancy[preferred_lot] < all_lot_capacities[preferred_lot]:
                assigned_lot = preferred_lot
            else:
                # try other lots on campus
                other_lots = [lot for lot in campus_lots if lot != preferred_lot]
                np.random.shuffle(other_lots)

                for lot in other_lots:
                    if lot_occupancy[lot] < all_lot_capacities[lot]:
                        assigned_lot = lot
                        break # found a spot

            if assigned_lot:
                # success -> assign spot, track session, update occupancy
                lot_occupancy[assigned_lot] += 1
                active_sessions[session_id] = assigned_lot

                # add lot_id to event and store it
                event_data = row._asdict()
                event_data['lot_id'] = assigned_lot
                processed_events.append(event_data)
            else:
                # failure -> all campus lots are full, reject this session
                rejected_session_ids.add(session_id)

    print(f"Simulation complete. Rejected {len(rejected_session_ids)} total sessions (no lots available)")

    final_df = pd.DataFrame(processed_events)

    # re-ordering the columns
    if not final_df.empty:
        cols = ['session_id', 'timestamp', 'lot_id', 'event_type', 'student_id', 'campus']
        # add flags if they exist
        cols.extend([c for c in final_df.columns if c.startswith('is')])
        final_df = final_df[cols]

    return final_df


def generate_synthetic_parking_data():
    """
    main function to generate and save parking data
    """

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # generate class schedules for last 3 years
    base_schedule_df = generate_synthetic_schedule(SEMESTERS[0]['start'], SEMESTERS[-1]['sem_end'])

    # add calendar flags to this schedule
    base_schedule_df['is_weekday'] = base_schedule_df['date'].apply(lambda d: d.weekday() < 5)
    base_schedule_df['is_weekend'] = base_schedule_df['date'].apply(lambda d: d.weekday() >= 5)
    base_schedule_df['is_holiday'] = base_schedule_df['date'].apply(lambda d: d in HOLIDAYS_DT)
    base_schedule_df['isSpecialDay'] = base_schedule_df['date'].apply(lambda d: d in SPECIAL_DAYS_DT)

    for term in SEMESTERS:
        print(f"Generating parking data for {term['name']}")

        term_start = pd.to_datetime(term['start']).date()
        class_end = pd.to_datetime(term['class_end']).date()
        sem_end = pd.to_datetime(term['sem_end']).date()

        # filter current term from the base schedule (consisting of 3 years schedule)
        term_dates = pd.date_range(start=term_start, end=sem_end, freq='D').date
        term_schedule = base_schedule_df[base_schedule_df['date'].isin(term_dates)].copy()

        # weekday/weekend/holiday separation
        active_day_schedule = term_schedule[term_schedule['is_weekday'] & ~term_schedule['is_holiday']].copy()
        off_day_schedule = term_schedule[term_schedule['is_weekend'] | term_schedule['is_holiday']].copy()

        unique_students = term_schedule[['student_id', 'campus']].drop_duplicates()
        weekend_dates = [d for d in term_dates if pd.to_datetime(d).weekday() >= 5 and d <= TODAY]

        weekend_rows = []
        if not unique_students.empty:
            for d in weekend_dates:
                for _, srow in unique_students.iterrows():
                    start_hour = np.random.randint(9, 15)
                    start_minute = np.random.choice([0, 30])
                    start_dt = pd.to_datetime(f"{start_hour:02d}:{start_minute:02d}")
                    end_dt = start_dt + timedelta(hours=2)
                    weekend_rows.append({
                        'student_id': srow['student_id'],
                        'campus': srow['campus'],
                        'day': '',
                        'start_time': start_dt.time(),
                        'end_time': end_dt.time(),
                        'date': d
                    })

        if weekend_rows:
            weekend_schedule_df = pd.DataFrame(weekend_rows)
            off_day_schedule = pd.concat([off_day_schedule, weekend_schedule_df], ignore_index=True)

        # simulation Period Definitions
        busy_end_date = term_start + timedelta(weeks=2)

        # busy Period (weekdays only)
        busy_schedule = active_day_schedule[active_day_schedule['date'] < busy_end_date]
        busy_events_df = simulate_events(busy_schedule, current_period='busy')

        # regular Period (weekdays only)
        regular_schedule = active_day_schedule[
            (active_day_schedule['date'] >= busy_end_date) & (active_day_schedule['date'] <= class_end)]
        regular_events_df = simulate_events(regular_schedule, current_period='regular')

        # exam Period
        exam_schedule = active_day_schedule[active_day_schedule['date'] > class_end]
        exam_events_df = simulate_events(exam_schedule, current_period='exam')

        # off-day - weekends and holidays - low demand period
        off_day_events_df = simulate_events(off_day_schedule, current_period='off_day', student_frac=OFF_DAY_FRACTION)

        # combine, sort, save
        print(busy_events_df.head())
        print(regular_events_df.head())
        term_events = pd.concat([busy_events_df, regular_events_df, exam_events_df, off_day_events_df])
        term_events = term_events.sort_values('timestamp')

        # filter the final combined data for the semester up to TODAY's date
        term_events = term_events[term_events['timestamp'].dt.date <= TODAY]

        # re-process the events and assign lots based on capacity
        # drop sesssion that can't park
        term_events_final = assign_lots_with_capacity(term_events)

        filename = os.path.join(OUTPUT_DIR, f"sfu_parking_logs_{term['name'].replace(' ', '_')}.csv")
        term_events_final.to_csv(filename, index=False)
        print(f"Successfully saved {len(term_events_final)} events for {term['name']} (up to {TODAY}) to {filename}")

    print("\nData generation complete. Ready for Spark/Parquet conversion.")


if __name__ == '__main__':
    sys.setrecursionlimit(2000)
    generate_synthetic_parking_data()