from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# 創建 SparkSession
spark = (SparkSession.builder
         .appName("ElectionAnalysis")
         .master("local[*]")
         .config("spark.jars.packages", 
                 "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
         .config("spark.jars", "/home/jack/RealtimeVotingEngineering/postgresql-42.7.3.jar")
         .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
         .getOrCreate())

# 定義 votes_topic schema
vote_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("voting_time", TimestampType(), True),
    StructField("vote", IntegerType(), True)
])

# 定義 voters_topic schema
voter_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("voter_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("registration_number", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("cell_number", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("registered_age", IntegerType(), True),
    StructField("candidate_name", StringType(), True),
    StructField("party_affiliation", StringType(), True)
])

# 從 Kafka 讀取 votes_topic 數據
votes_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "votes_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), vote_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("voting_time", "1 minute")

# 從 Kafka 讀取 voters_topic 數據
voters_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "voters_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), voter_schema).alias("data")) \
    .select("data.*") \
    .withColumn("voter_time", current_timestamp()) \
    .withWatermark("voter_time", "1 minute")

# 將 votes_df 和 voters_df 進行連接
enriched_votes_df = votes_df.alias("votes").join(
  voters_df.alias("voters"),
  expr("""
    votes.voter_id = voters.voter_id AND
    voters.voter_time >= votes.voting_time AND
    voters.voter_time <= votes.voting_time + interval 1 hour
    """),
  "leftOuter"
)

# 確保所有列都有正確的前綴
enriched_votes_df = enriched_votes_df.select(
    "votes.*",
    "voters.address",
    "voters.voter_time"
)

# Data preprocessing: type casting
enriched_votes_df = enriched_votes_df.withColumn('vote', col('vote').cast(IntegerType()))

# Aggregate turnout by location
turnout_by_location = enriched_votes_df.groupBy(
    window(col("voting_time"), "1 minute"), 
    col("address.state")
).agg(_sum("vote").alias("total_votes"))

# Write aggregated data to Kafka topics ('aggregated_turnout_by_location')
turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_turnout_by_location") \
    .option("checkpointLocation", "/home/jack/RealtimeVotingEngineering/checkpoints/checkpoint2") \
    .outputMode("append") \
    .start()

# 打印 turnout_by_location 的聚合結果到控制台
turnout_by_location.printSchema()
turnout_by_location_to_console = turnout_by_location \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Await termination for the streaming queries
turnout_by_location_to_kafka.awaitTermination()
turnout_by_location_to_console.awaitTermination()