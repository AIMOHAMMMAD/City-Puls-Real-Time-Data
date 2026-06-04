# check_kafka_topics.py
import subprocess

try:
    # التحقق من التوبيكات الموجودة
    result = subprocess.run([
        "docker", "exec", "conn-kafka-1", 
        "kafka-topics", 
        "--list", 
        "--bootstrap-server", "localhost:9092"
    ], capture_output=True, text=True)
    
    print("📋 التوبيكات المتاحة في Kafka:")
    topics = result.stdout.strip().split('\n')
    for topic in topics:
        if topic:
            print(f"  - {topic}")
    
    # التحقق من وجود توبيك التوظيف
    if "employment_indicators" in topics:
        print("✅ توبيك employment_indicators موجود")
    else:
        print("❌ توبيك employment_indicators غير موجود")
        
except Exception as e:
    print(f"❌ خطأ: {e}")
