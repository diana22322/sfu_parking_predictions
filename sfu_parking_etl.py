import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, functions, types

parking_schema = types.StructType([
    types.StructField('session_id', types.StringType(), False),
    types.StructField('timestamp', types.StringType(), False),
    types.StructField('lot_id', types.StringType(), False),
    types.StructField('event_type', types.StringType(), False),
    types.StructField('student_id', types.StringType(), False),
    types.StructField('campus', types.StringType(), False),
    types.StructField('isHoliday', types.StringType(), True),
    types.StructField('isWeekend', types.StringType(), True),
    types.StructField('isExamWeek', types.StringType(), True),
    types.StructField('isFirst2Weeks', types.StringType(), True),
    types.StructField('isSpecialDay', types.StringType(), True)
])

def run_etl_job(spark, input, output):
    print("Starting ETL job ...")
    raw_df = spark.read.csv(input, header=True, schema=parking_schema, sep=',')

    cleaned_df = raw_df.select(
        functions.to_timestamp(raw_df['timestamp'], "yyyy-MM-dd HH:mm:ss.SSSSSS").alias('timestamp'),
        raw_df['student_id'].cast(types.IntegerType()).alias('student_id'),
        functions.upper(functions.trim(raw_df['lot_id'])).alias('lot_id'),
        functions.upper(functions.trim(raw_df['event_type'])).alias('event_type'),
        functions.upper(functions.trim(raw_df['campus'])).alias('campus'),
        raw_df['session_id'],
        raw_df['isHoliday'].cast(types.BooleanType()),
        raw_df['isWeekend'].cast(types.BooleanType()),
        raw_df['isExamWeek'].cast(types.BooleanType()),
        raw_df['isFirst2Weeks'].cast(types.BooleanType()),
        raw_df['isSpecialDay'].cast(types.BooleanType())
    )

    final_df = cleaned_df.withColumn('date', functions.to_date(cleaned_df['timestamp']))

    final_df.write.mode('overwrite').partitionBy("campus", "date").parquet(output)
    print("ETL job completed successfully ...")

def main():
    INPUT_S3_PATH = "s3://c732-sfu-parking-data-lake/raw/historical"
    OUTPUT_S3_PATH = "s3://c732-sfu-parking-data-lake/processed/historical"

    spark = SparkSession.builder.appName('SFU parking ETL').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    try:
        run_etl_job(spark, INPUT_S3_PATH, OUTPUT_S3_PATH)
    except Exception as e:
        print(f"ETL job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()