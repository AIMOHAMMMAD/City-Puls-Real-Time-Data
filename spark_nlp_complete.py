import sys
import time
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Spark NLP Imports
import sparknlp
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    Tokenizer,
    Normalizer,
    BertForSequenceClassification
)

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CityPulseSpark")

def create_spark_session():
    return SparkSession.builder \
        .appName("CityPulse_Arabic_Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,"
                "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    logger.info(f"Spark NLP Version: {sparknlp.version()}")
    logger.info("🚀 Starting CityPulse Processing Engine (Enhanced Schema)...")

    # 1. إعداد الـ Schema المتكاملة (تطابق Producer)
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("country", StringType(), True), # ✅ مهم جداً للخريطة
        StructField("timestamp", StringType(), True),
        StructField("manual_sentiment", StringType(), True) # للاختبار والمقارنة
    ])

    # 2. قراءة الـ Stream من Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "social-media-data") \
        .option("startingOffsets", "latest") \
        .load()

    # تحويل البيانات من JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # ملء القيم الفارغة لتجنب الأخطاء
    parsed_df = parsed_df.na.fill({"country": "Jordan", "text": ""})

    # ---------------------------------------------------------
    # 3. بناء خط أنابيب المعالجة (NLP Pipeline)
    # ---------------------------------------------------------
    
    document_assembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    # تنظيف النصوص العربية
    normalizer = Normalizer() \
        .setInputCols(["token"]) \
        .setOutputCol("normalized") \
        .setLowercase(False) \
        .setCleanupPatterns(["[^\w\d\s]"]) # إزالة الرموز الزائدة

    # الموديل متعدد اللغات (الأقوى للعربية)
    bert_sentiment = BertForSequenceClassification.pretrained("bert_sequence_classifier_multilingual_sentiment", "xx") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("class") \
        .setCaseSensitive(False) \
        .setCoalesceSentences(True)

    nlp_pipeline = Pipeline(stages=[
        document_assembler,
        tokenizer,
        normalizer,
        bert_sentiment
    ])

    # ---------------------------------------------------------
    # 4. التطبيق والتحويل
    # ---------------------------------------------------------
    
    empty_df = spark.createDataFrame([[""]], ["text"])
    pipeline_model = nlp_pipeline.fit(empty_df)

    processed_df = pipeline_model.transform(parsed_df)

    # ---------------------------------------------------------
    # 5. استخراج النتائج النهائية وتنسيقها
    # ---------------------------------------------------------
    
    output_df = processed_df.select(
        col("text"),
        col("source"),
        col("country"), # ✅ الآن سيتم حفظ الدولة
        col("class.result").getItem(0).alias("sentiment_label"), 
        col("timestamp"),
        current_timestamp().alias("processed_at")
    )

    # 6. الكتابة إلى MongoDB و Console
    logger.info("📡 Pipeline Ready. Writing to MongoDB...")
    
    mongo_query = output_df.writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_mongo") \
        .option("spark.mongodb.connection.uri", "mongodb://citypulse-mongo:27017") \
        .option("spark.mongodb.database", "citypulse_db") \
        .option("spark.mongodb.collection", "sentiment_results") \
        .start()
    
    # للعرض في الـ Logs فقط (للتأكد)
    console_query = output_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()