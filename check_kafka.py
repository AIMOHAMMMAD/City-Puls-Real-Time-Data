from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

def check_kafka():
    print("🔄 جاري فحص اتصال Kafka...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:29092", 
            request_timeout_ms=5000
        )
        topics = admin_client.list_topics()
        print("✅ اتصال Kafka: ناجح!")
        
        target_topic = "social-media-data"
        if target_topic in topics:
            print(f"✅ القناة '{target_topic}': موجودة وجاهزة.")
        else:
            print(f"⚠️ القناة '{target_topic}': غير موجودة (سيقوم المنتج بإنشائها).")
            
    except NoBrokersAvailable:
        print("❌ فشل الاتصال: Kafka لا يعمل! تأكد من تشغيل Docker.")
    except Exception as e:
        print(f"❌ خطأ غير متوقع: {e}")

if __name__ == "__main__":
    check_kafka()