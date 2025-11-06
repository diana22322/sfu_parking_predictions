import numpy as np
import pandas as pd
import os
import sys
import uuid
from datetime import timedelta, date, time

"""
Synthetic Historical Parking Data Generator
-------------------------------------------
Generates synthetic parking data for the Burnaby campus, covering Fall and Spring
semesters from Fall 2023 to the present.
The data is designed to approximate realistic parking patterns and occupancy trends,
based on typical campus usage behavior and domain knowledge.
"""

OUTPUT_DIR = 'historic_sfu_parking_logs'
STUDENT_COUNT = 25000       # Unique student ID's to simulate

# Define today's date (the point where data generation must stop)
TODAY = date(2025, 11, 6)
OFF_DAY_FRACTION = 0.12 # ~12% of activate students show up on the weekends too

# List of stat holidays affecting campus activities/parking
STAT_HOLIDAYS = [
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
    '2023-10-05', '2023-10-06',
    '2024-06-11', '2024-06-14',
    '2024-10-03', '2024-10-04',
    '2025-06-10', '2025-06-13',
    '2025-10-02', '2025-10-03'
]

SPECIAL_DAYS_DT = {pd.to_datetime(d).date() for d in SPECIAL_DAYS}

BURNABY_LOTS = {
    'North' : 1000, 'East' : 800, 'Central' : 600, 'West' : 500, 'South' : 400
}
BURNABY_WEIGHTS = [0.35, 0.30, 0.15, 0.10, 0.10]

# --- NEW SURREY CAMPUS CONFIG ---
SURREY_LOTS = {
    'SRYC' : 350,  # Central City - Level P3
    'SRYE' : 250   # Underground Parkade
}
# Surrey lot preference (SRYC likely gets more traffic due to location)
SURREY_WEIGHTS = [0.60, 0.40]

# --- COMBINED LOT AND CAMPUS DATA STRUCTURE ---
CAMPUS_LOTS = {
    'Burnaby': {'lots': BURNABY_LOTS, 'weights': BURNABY_WEIGHTS, 'max_students': int(STUDENT_COUNT * 0.85)},
    'Surrey': {'lots': SURREY_LOTS, 'weights': SURREY_WEIGHTS, 'max_students': int(STUDENT_COUNT * 0.15)} # Assume ~15% of students primarily use Surrey
}

# extract names for simpler access later
BURNABY_LOT_NAMES = list(BURNABY_LOTS.keys())
SURREY_LOT_NAMES = list(SURREY_LOTS.keys())


# Semester definitions (start, end, exam boundaries)
# two-week exam period at the end of the semester
SEMESTERS = [
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

    # Define the observed worst peak window for arrival (11:15 AM to 12:30 PM)
    PEAK_START = time(10, 30)
    PEAK_END = time(12, 30)

    #*****************
    # Reduce sample of students for low-demand periods (used for off_day). Only subset of students come to campus on weekends on holidays
    if student_frac < 1.0:
        active_students = schedule_df['student_id'].unique()
        sample_size = int(len(active_students) * student_frac)
        schedule_df = schedule_df[schedule_df['student_id'].isin(
            np.random.choice(active_students, size=sample_size, replace=False)
        )]

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
        departure_buffer = np.random.uniform(60, 180) # default 1-3 hours
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
        if current_campus == 'Burnaby':
            lot_names = BURNABY_LOT_NAMES
            lot_weights = BURNABY_WEIGHTS
        elif current_campus == 'Surrey':
            lot_names = SURREY_LOT_NAMES
            lot_weights = SURREY_WEIGHTS
        else:
            continue

        assigned_lot = np.random.choice(lot_names, p=lot_weights)

        # append arrival event
        events.append({
            'session_id': session_id,
            'timestamp': arrival_time,
            'lot_id': assigned_lot,
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
                'lot_id': assigned_lot,
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

    # Execution

def generate_synthetic_parking_data():
    """
    main function to generate and save parking data
    """

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # generate class schedules for last 2 years
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

        # filter current term from the base schedule (consisting of 2 years schedule)
        term_dates = pd.date_range(start=term_start, end=sem_end, freq='D').date
        term_schedule = base_schedule_df[base_schedule_df['date'].isin(term_dates)].copy()

        # weekday/weekend/holiday separation
        active_day_schedule = term_schedule[term_schedule['is_weekday'] & ~term_schedule['is_holiday']].copy()
        off_day_schedule = term_schedule[term_schedule['is_weekend'] | term_schedule['is_holiday']].copy()
        off_day_schedule = off_day_schedule.drop_duplicates(subset=['date'])

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

        filename = os.path.join(OUTPUT_DIR, f"sfu_parking_logs_{term['name'].replace(' ', '_')}.csv")
        term_events.to_csv(filename, index=False)
        print(f"Successfully saved {len(term_events)} events for {term['name']} (up to {TODAY}) to {filename}")

    print("\nData generation complete. Ready for Spark/Parquet conversion.")


if __name__ == '__main__':
    sys.setrecursionlimit(2000)
    generate_synthetic_parking_data()

