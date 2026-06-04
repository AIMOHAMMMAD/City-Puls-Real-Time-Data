# employment_streaming.py
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, MapType
        
        print("بدء تشغيل Spark Streaming للتوظيف...")
        
        # جلسة Spark
        spark = SparkSession.builder \
            .appName("EmploymentStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        
        # Schema لبيانات التوظيف
        employment_schema = StructType([
            StructField("country", StringType()),
            StructField("year", IntegerType()),
            StructField("unemployment_rate", DoubleType()),
            StructField("youth_unemployment", DoubleType()),
            StructField("sector_employment", MapType(StringType(), DoubleType()))
        ])
        
        print("جاري الاتصال بـ Kafka لبيانات التوظيف...")
        
        # قراءة بيانات التوظيف من Kafka
        employment_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:29092")
            .option("subscribe", "employment_indicators")
            .option("startingOffsets", "earliest")  # اقرأ من البداية
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), employment_schema).alias("data"))
            .select("data.*"))
        
        # دالة لحفظ بيانات التوظيف في MongoDB
        def save_employment_batch(df, epoch_id):
            try:
                count = df.count()
                if count > 0:
                    print(f"دفعة توظيف #{epoch_id} - {count} سجل")
                    
                    # عرض عينة من البيانات
                    df.show(3, truncate=False)
                    
                    # حفظ في MongoDB
                    pandas_df = df.toPandas()
                    
                    from pymongo import MongoClient
                    client = MongoClient("mongodb://localhost:27017/")
                    db = client["citypulse"]
                    collection = db["employment_raw"]
                    
                    records = pandas_df.to_dict('records')
                    if records:
                        collection.insert_many(records)
                        print(f"تم حفظ {len(records)} سجل توظيف في MongoDB")
                        
            except Exception as e:
                print(f"خطأ في حفظ التوظيف: {e}")
        
        # تشغيل التدفق
        query = (employment_df.writeStream
            .outputMode("append")
            .foreachBatch(save_employment_batch)
            .option("checkpointLocation", "./chkpt_employment")
            .start())
        
        print("جاهز لاستقبال بيانات التوظيف من Kafka...")
        print("شغل: python employment_producer_complete.py")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"خطأ رئيسي: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
