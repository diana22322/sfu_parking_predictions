import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    BooleanType,
    DateType
)

from pyspark.sql.window import Window
from datetime import date

# Input paths for *all* our Parquet data
HISTORICAL_S3_PATH = "s3://c732-sfu-parking-data-lake/processed/historical/"
REALTIME_S3_PATH = "s3://c732-sfu-parking-data-lake/raw/realtime/" # From Firehose

# Output path for the new, combined feature set
OUTPUT_S3_PATH = "s3://c732-sfu-parking-data-lake/processed/training_features/"

# Real-time Schema exactly as it comes from Firehose (Strings)
REALTIME_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("lot_id", StringType(), True),
    StructField("campus", StringType(), True),
    StructField("student_id", StringType(), True),
    StructField("plate_number", StringType(), True)
])

STAT_HOLIDAYS = [
    "2022-02-21", "2022-04-15",
    "2022-10-10", "2022-11-11",
    "2023-10-09", "2023-11-13",
    "2024-02-12", "2024-03-29",
    "2024-10-14", "2024-11-11",
    "2025-02-17", "2025-04-18",
    "2025-10-13", "2025-11-11"
]
HOLIDAY_DATES = [date.fromisoformat(d) for d in STAT_HOLIDAYS]

# helper functions
def standardize_realtime_data(df):
    """
    Casts and renames the string based reatime data to match historical schema
    """
    print("Standardizing real-time schema...")
    ts_pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
    df_std = df.select(
        F.col("session_id"),
        F.to_timestamp(F.col("timestamp"), ts_pattern).alias("event_ts"),  # cast string to timestamp
        F.upper(F.trim(F.col("lot_id"))).alias("lot_id"),
        F.upper(F.trim(F.col("campus"))).alias("campus"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type"),
        F.col("student_id").cast(IntegerType()).alias("student_id"),  # cast string to int
        F.col("plate_number"),
        # create null for these flags, will fill these later based on date
        F.lit(None).cast(BooleanType()).alias("is_holiday"),
        F.lit(None).cast(BooleanType()).alias("is_weekend"),
        F.lit(None).cast(BooleanType()).alias("is_exam_week"),
        F.lit(None).cast(BooleanType()).alias("is_first_2_weeks"),
        F.lit(None).cast(BooleanType()).alias("is_special_day"),
        F.to_date(F.to_timestamp(F.col("timestamp"), ts_pattern)).alias('date')
    )
    return df_std

def fill_time_gaps(df_features):
    spark = df_features.sparkSession

    df_ts = df_features.withColumn("timestamp", F.col("window.start"))

    min_max = df_ts.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]

    if not min_max["min_ts"]:
        return df_features

    time_skeleton = spark.range(1).select(
        F.explode(
            F.sequence(
                F.lit(min_max["min_ts"]),
                F.lit(min_max["max_ts"]),
                F.expr("interval 15 minutes")
            )
        ).alias("window_start")
    )

    lot_skeleton = df_ts.select("lot_id", "campus").distinct()
    full_skeleton = lot_skeleton.crossJoin(time_skeleton)

    df_joined = full_skeleton.join(
        df_ts,
        (full_skeleton.lot_id == df_ts.lot_id) &
        (full_skeleton.window_start == df_ts.timestamp),
        "left"
    ).select(
        full_skeleton.lot_id,
        full_skeleton.campus,
        full_skeleton.window_start.alias("timestamp"),
        df_ts.occupancy_now,
        df_ts.is_holiday,
        df_ts.is_weekend,
        df_ts.is_exam_week
    )

    w_ff = Window.partitionBy("lot_id", "campus").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_filled = df_joined.withColumn(
        "occupancy_now", F.last("occupancy_now", ignorenulls=True).over(w_ff)
    ).withColumn(
        "is_holiday", F.last("is_holiday", ignorenulls=True).over(w_ff)
    ).withColumn(
        "is_weekend", F.last("is_weekend", ignorenulls=True).over(w_ff)
    ).withColumn(
        "is_exam_week", F.last("is_exam_week", ignorenulls=True).over(w_ff)
    )

    df_filled = df_filled.fillna(0, subset=["occupancy_now"])
    df_filled = df_filled.withColumn("window", F.window("timestamp", "15 minutes"))
    return df_filled

def create_features_and_labels(df):
    """
    Generates features and labels from the complete event log.
    """
    print('Generating event-level occupancy changes...')

    df = df.withColumn('date_str', F.to_date('event_ts'))

    # calculate occupancy change for each event
    df_with_change = df.withColumn(
        "occupancy_change",
        F.when(F.col('event_type') == 'ARRIVAL', 1).otherwise(-1)
    )

    # calculate running occupancy at the time of each event
    # this window caculates the cumulative sum of changes for each lot, ordered by time
    # sum all occupancy changes leading upto and including this event
    w_running_occ = Window.partitionBy('lot_id', 'date_str') \
                      .orderBy('event_ts') \
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_with_running_occ = df_with_change.withColumn(
        'occupancy_now',
        F.sum('occupancy_change').over(w_running_occ)
    )

    # aggregate data into 15 min windows
    # we will use the last known state from each window as feature for that window
    print(f"Aggregating data into 15 minute feature windows...")
    df_features = df_with_running_occ.groupBy(
        'lot_id', 'campus',
        F.window('event_ts', '15 minutes').alias('window')
    ).agg(
        F.last('occupancy_now').alias('occupancy_now'),
        F.last("is_holiday", ignorenulls=True).alias("is_holiday"),
        F.last("is_weekend", ignorenulls=True).alias("is_weekend"),
        F.last("is_exam_week", ignorenulls=True).alias("is_exam_week")
        #
    )

    # create time-based features from the window start time
    
    df_features = fill_time_gaps(df_features)
    df_features = df_features.withColumn('timestamp', F.col('window.start'))

    df_features = df_features.withColumn("date", F.to_date("timestamp"))

    df_features = df_features.withColumn(
        "is_holiday",
        F.col("date").isin(HOLIDAY_DATES)
    )

    df_features = df_features.withColumn(
        "is_weekend",
        F.dayofweek("date").isin([1, 7])  # Spark: 1=Sun, 7=Sat
    )

    df_features = df_features.withColumn('day_of_week', F.dayofweek(F.col('timestamp')))
    df_features = df_features.withColumn('hour_of_day', F.hour(F.col('timestamp')))

    # create labels (future data)
    print("Generating future labels...")

    # label 1 - occupancy in 30 minutes
    # create a new df of features, but shift its timestamp back by 30 minutes
    # join with the current window
    # e.g. occupancy_now for 10:30am becomes occupancy_plus_30m label for 10:00am row
    df_label_30m = df_features.withColumn(
        'join_window',
        F.window(F.col('timestamp') - F.expr('INTERVAL 15 MINUTES'), '15 minutes')
    ).select(
        F.col('lot_id').alias('l1_lot_id'),
        F.col('join_window'),
        F.col('occupancy_now').alias('occupancy_plus_15m')
    )

    # label 2 - departures in the next 15 minutes
    # count all departures and bucket them into 15-min window
    df_departures = df_with_change.filter(F.col('event_type') == 'DEPARTURE')

    df_label_15m = df_departures.groupBy(
        'lot_id',
        F.window('event_ts', '15 minutes').alias('join_window_d')
    ).agg(
        F.count("*").alias('departures_in_15m')
    ).select(
        F.col('lot_id').alias('l2_lot_id'),
        F.col('join_window_d'),
        F.col('departures_in_15m')
    )

    # join features and labels
    final_df = df_features.join(
        df_label_30m,
        (df_features.lot_id == df_label_30m.l1_lot_id) & (df_features.window == df_label_30m.join_window),
        "left"
    ).join(
        df_label_15m,
        (df_features.lot_id == df_label_15m.l2_lot_id) & (df_features.window == df_label_15m.join_window_d),
        "left"
    )

    # filter
    final_df = final_df.select(
        "lot_id", "campus", "timestamp", "occupancy_now", "day_of_week", "hour_of_day",
        "is_holiday", "is_weekend", "is_exam_week", "occupancy_plus_15m", "departures_in_15m"
    ).fillna(0, subset=["occupancy_plus_15m", "departures_in_15m"])

    return final_df

def main():
    spark = SparkSession.builder.appName('SFU Parking ML Feature Engineering').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    try:
        print("--- DEBUG MODE START ---")

        # Read historical data
        print(f"Reading historical data from {HISTORICAL_S3_PATH}...")
        hist_df_raw = spark.read.parquet(HISTORICAL_S3_PATH)

        hist_df_raw.select("date", "isHoliday").groupBy("isHoliday").count().show()
        hist_df_raw.groupBy("date").agg(F.max("isHoliday").alias("isHoliday")).orderBy("date").show(100)
        
        # Map correct schema & rename columns
        hist_df = hist_df_raw.select(
            F.col("session_id"),
            F.col("timestamp").alias("event_ts"), # Rename timestamp -> event_ts
            F.upper(F.trim(F.col("lot_id"))).alias("lot_id"),
            F.upper(F.trim(F.col("campus"))).alias("campus"),
            F.upper(F.trim(F.col("event_type"))).alias("event_type"),
            F.col("student_id").cast(IntegerType()),
            F.lit(None).cast(StringType()).alias("plate_number"), # Missing in historical
            # Rename Boolean Flags
            F.col("isHoliday").alias("is_holiday"),
            F.col("isWeekend").alias("is_weekend"),
            F.col("isExamWeek").alias("is_exam_week"),
            F.col("isFirst2Weeks").alias("is_first_2_weeks"),
            F.col("isSpecialDay").alias("is_special_day"),
            F.col("date")
        )

        calendar_df = hist_df.select(
            F.col("date").alias("cal_date"),
            F.col("is_holiday").alias("cal_is_holiday"),
            F.col("is_weekend").alias("cal_is_weekend"),
            F.col("is_exam_week").alias("cal_is_exam_week"),
            F.col("is_first_2_weeks").alias("cal_is_first_2_weeks"),
            F.col("is_special_day").alias("cal_is_special_day")
        ).dropDuplicates(["cal_date"])

        # Read realtime data
        print(f"Reading real-time data...")
        try:
            rt_df = spark.read.option("recursiveFileLookup", "true").schema(REALTIME_SCHEMA).parquet(REALTIME_S3_PATH)
            if rt_df.rdd.isEmpty():
                print("Real-time empty. Creating dummy DF.")
                rt_df_std = spark.createDataFrame([], hist_df.schema)
            else:
                rt_df_std = standardize_realtime_data(rt_df)
                rt_df_std = rt_df_std.join(
                    calendar_df,
                    rt_df_std.date == calendar_df.cal_date,
                    "left"
                ).drop("cal_date")
                rt_df_std = rt_df_std.withColumn(
                    "is_holiday",
                    F.coalesce(F.col("is_holiday"), F.col("cal_is_holiday"))
               ).withColumn(
                    "is_weekend",
                    F.coalesce(F.col("is_weekend"), F.col("cal_is_weekend"))
                ).withColumn(
                    "is_exam_week",
                    F.coalesce(F.col("is_exam_week"), F.col("cal_is_exam_week"))
                ).withColumn(
                    "is_first_2_weeks",
                    F.coalesce(F.col("is_first_2_weeks"), F.col("cal_is_first_2_weeks"))
                ).withColumn(
                    "is_special_day",
                    F.coalesce(F.col("is_special_day"), F.col("cal_is_special_day"))
                ).drop(
                    "cal_is_holiday",
                    "cal_is_weekend",
                    "cal_is_exam_week",
                    "cal_is_first_2_weeks",
                    "cal_is_special_day"
                )
        except Exception:
            print("Real-time path missing. Creating dummy DF.")
            rt_df_std = spark.createDataFrame([], hist_df.schema)

        # combibe into one massive event log
        all_events_df = hist_df.unionByName(rt_df_std, allowMissingColumns=True)

        print(f"Count before filtering nulls: {all_events_df.count()}")

        all_events_df = all_events_df.filter(F.col('event_ts').isNotNull()).cache()

        print(f"Count after filtering nulls: {all_events_df.count()}")

        print(f"Total combined events: {all_events_df.count()}")

        final_training_set = create_features_and_labels(all_events_df)

        # write the training set to S3 as a single Parquet file
        print(f"Writing final training set to {OUTPUT_S3_PATH}..")
        final_training_set.coalesce(1).write.mode('overwrite').parquet(OUTPUT_S3_PATH)

        print('Feature Engineering complete!')
        print(f"Training set schema:")
        final_training_set.printSchema()

    except Exception as e:
        print(f"Feature engineering job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()