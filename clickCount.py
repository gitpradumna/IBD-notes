from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when
from datetime import datetime

spark = SparkSession.builder.appName("ClickCountsTimeOfDay").getOrCreate()
sc = spark.sparkContext

BUCKET_NAME = "pradumna-iitm-ibd-w4"
INPUT_PATH = f"gs://{BUCKET_NAME}/click_file.txt"
OUTPUT_RDD_PATH = f"gs://{BUCKET_NAME}/output_rdd"
OUTPUT_DF_PATH = f"gs://{BUCKET_NAME}/output_df"

# -----------------------------------------
# APPROACH 1: RDD IMPLEMENTATION
# -----------------------------------------

rawRdd = sc.textFile(INPUT_PATH)
header = rawRdd.first()
dataRdd = rawRdd.filter(lambda row: row != header)

def get_time_bin(row):
    try:
        parts = row.split(",")
        if len(parts) != 2:
            return None
        
        tsStr = parts[1].strip()
        dt = datetime.strptime(tsStr, "%Y-%m-%d %H:%M:%S")
        hr = dt.hour
        
        if 0 <= hr < 6:
            return ("0-6 hours", 1)
        elif 6 <= hr < 12:
            return ("6-12 hours", 1)
        elif 12 <= hr < 18:
            return ("12-18 hours", 1)
        else:
            return ("18-24 hours", 1)
    except:
        return None

rddCounts = (dataRdd
              .map(get_time_bin)
              .filter(lambda x: x is not None)
              .reduceByKey(lambda a, b: a + b))

rddCounts.coalesce(1).saveAsTextFile(OUTPUT_RDD_PATH)


# -----------------------------------------
# APPROACH 2: DATAFRAME IMPLEMENTATION
# -----------------------------------------

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
dfWithHour = df.withColumn("hour", hour(col("timestamp").cast("timestamp")))

dfBins = dfWithHour.withColumn(
    "time_interval",
    when((col("hour") >= 0) & (col("hour") < 6), "0-6 hours")
    .when((col("hour") >= 6) & (col("hour") < 12), "6-12 hours")
    .when((col("hour") >= 12) & (col("hour") < 18), "12-18 hours")
    .otherwise("18-24 hours")
)

dfCounts = dfBins.groupBy("time_interval").count()
dfCounts.coalesce(1).write.csv(OUTPUT_DF_PATH, header=True)
spark.stop()



