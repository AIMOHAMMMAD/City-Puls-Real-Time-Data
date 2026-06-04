# test_spark.py
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# جلسة Spark مبسطة جداً
spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# بيانات اختبارية بسيطة
data = [("John", 25), ("Jane", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

print("✅ Spark يعمل بشكل صحيح!")
print("📊 البيانات:")
df.show()

spark.stop()
