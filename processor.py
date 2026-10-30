import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StructField, StringType, FloadType
from database import sessionLocal, LiveTelemetry

def start_processing():
    spark=SparkSession.builder \
        .appName("RAM-Streaming-Processor")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()

    raw_schema=StructType([
        StructField("device_id",StringType(),True),
        StructField("reading",FloadType(),True)
    ])

    kafka_stream=spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "telemetry-stream") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream=kafka_stream\
        .selectExpr("CAST(value AS STRING) as json_data")\
        .select(from_json(col("json_data"),raw_schema).alias("parsed"))\
        .select("parsed.*")

    def write_to_ram(df,batch_id):
        records=df.collect()
        if not records:
            return
            
        db = sessionLocal()
        try:
            for row in records:
                # Compute live status assessment logic
                status_flag = "ALERT" if row["reading"] > 75.0 else "OK"
                
                db_record = LiveTelemetry(
                    device_id=row["device_id"],
                    reading=row["reading"],
                    status=status_flag
                )
                db.add(db_record)
            db.commit()
            print(f"--> [Spark] Successfully committed batch #{batch_id} containing {len(records)} entries to RAM.")
        except Exception as e:
            print(f"Error persisting batch {batch_id}: {e}")
            db.rollback()
        finally:
            db.close()

        query = parsed_stream.writeStream \
        .foreachBatch(write_to_ram) \
        .start()

        query.awaitTermination()

if __name__ == "__main__":
    start_processing()