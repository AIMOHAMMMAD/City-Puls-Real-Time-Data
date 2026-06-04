# streaming_with_mongo.py
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json, udf, current_timestamp
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        print("🚀 بدء تشغيل Spark Streaming مع MongoDB...")
        
        spark = SparkSession.builder \
            .appName("CityPulse") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        
        # دالة تحليل المشاعر
        def analyze_sentiment(text):
            if not text: return "neutral"
            text = text.lower()
            pos_words = ["متفائل", "تمام", "الحمد لله", "أفضل", "أمل", "سعيد", "ان شاء الله"]
            neg_words = ["قلق", "صعب", "متوتر", "ضيق", "مرهق", "قلقان", "مكتئب"]
            
            pos = any(word in text for word in pos_words)
            neg = any(word in text for word in neg_words)
            
            if pos and not neg: return "positive"
            elif neg and not pos: return "negative"
            else: return "neutral"
        
        sentiment_udf = udf(analyze_sentiment, StringType())
        
        # Schema
        schema = StructType([
            StructField("country", StringType()),
            StructField("year", IntegerType()),
            StructField("text", StringType()),
            StructField("age", IntegerType()),
            StructField("gender", StringType())
        ])
        
        # قراءة من Kafka
        df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:29092")
            .option("subscribe", "mental_health_live")
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*")
            .withColumn("sentiment", sentiment_udf(col("text")))
            .withColumn("processed_time", current_timestamp()))
        
        # دالة الحفظ في MongoDB
        def save_batch(df, epoch_id):
            try:
                count = df.count()
                if count > 0:
                    print(f"💾 دفعة #{epoch_id} - جاري حفظ {count} سجل في MongoDB...")
                    
                    # تحويل إلى Pandas وحفظ في MongoDB
                    pandas_df = df.toPandas()
                    
                    from pymongo import MongoClient
                    client = MongoClient("mongodb://localhost:27017/")
                    db = client["citypulse"]
                    collection = db["mental_enriched"]
                    
                    records = pandas_df.to_dict('records')
                    collection.insert_many(records)
                    
                    print(f"✅ تم حفظ {len(records)} سجل في MongoDB")
                    df.show(3, truncate=False)
                    
            except Exception as e:
                print(f"⚠️  خطأ في الحفظ: {e}")
        
        # تشغيل التدفق مع الحفظ في MongoDB
        query = (df.writeStream
            .outputMode("append")
            .foreachBatch(save_batch)
            .option("checkpointLocation", "./chkpt_mongo")
            .start())
        
        print("🎯 النظام جاهز - في انتظار البيانات الجديدة...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ خطأ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
