import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# --- إعدادات الاتصال ---
KAFKA_TOPIC = "social-media-data"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

# --- بنك الكلمات المطور (لتوليد نصوص لا نهائية) ---
SUBJECTS = ["أنا", "الشباب", "الخريجين", "الناس", "المجتمع", "الكل", "الجيل", "الموظفين", "الطلاب"]
VERBS = ["يعاني من", "خايف من", "بفكر بـ", "تعب من", "مش ملاقي", "ببحث عن", "بستنى", "فقد الأمل بـ"]

# مواضيع السلبية (بطالة وقلق)
TOPICS_NEGATIVE = [
    "البطالة", "القلق", "الغلاء", "المستقبل المجهول", "الديون", 
    "ضغط الشغل", "التوتر", "الأسعار", "عدم التوظيف", "الواسطة"
]

# مواضيع الإيجابية (تفاؤل)
TOPICS_POSITIVE = [
    "النجاح", "الأمل", "الرزق", "التطوير", "الراحة", 
    "الإنجاز", "الاستقرار", "العائلة", "الويكند"
]

ADJECTIVES_NEG = [
    "الوضع صعب جداً", "اشي بقهر القلب", "تعبنا نفسياً", "ما في أمل بالمرة", 
    "كارثة حقيقية", "ظلم كبير", "الله يعين الجميع", "صرت افكر بالهجرة"
]

ADJECTIVES_POS = [
    "الحمد لله دائماً", "متفائل خير", "فرصة حلوة", "بداية جديدة", 
    "إنجاز عظيم", "يوم جميل", "طاقة ايجابية"
]

COUNTRIES = ["Jordan", "Saudi Arabia", "Egypt", "UAE", "Lebanon", "Kuwait", "Qatar", "Oman", "Iraq"]

# ---------------------------------------------------------
# 1. دالة الاتصال (مع كشف الأخطاء)
# ---------------------------------------------------------
def create_producer():
    print(f"🔄 جاري محاولة الاتصال بـ Kafka على: {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # إعدادات لزيادة الثبات
            reconnect_backoff_ms=1000, 
            request_timeout_ms=5000
        )
        print("✅ تم الاتصال بنجاح! المنتج جاهز للعمل.")
        return producer
    except Exception as e:
        print(f"❌ فشل الاتصال بـ Kafka! تأكد أن الحاوية تعمل.")
        print(f"⚠️ تفاصيل الخطأ: {e}")
        return None

# ---------------------------------------------------------
# 2. مولد النصوص الذكي
# ---------------------------------------------------------
def generate_smart_tweet():
    # 70% رسائل سلبية (لإظهار المشكلة في الداشبورد)، 30% إيجابية
    if random.random() < 0.7:
        # سيناريو سلبي (بطالة/قلق)
        topic = random.choice(TOPICS_NEGATIVE)
        text = f"{random.choice(SUBJECTS)} {random.choice(VERBS)} {topic}.. {random.choice(ADJECTIVES_NEG)}"
        sentiment = "Negative"
    else:
        # سيناريو إيجابي
        topic = random.choice(TOPICS_POSITIVE)
        text = f"{topic} هو الأساس.. {random.choice(ADJECTIVES_POS)}"
        sentiment = "Positive"

    # إضافة ضجيج عشوائي (هاشتاجات) لتبدو حقيقية
    hashtags = f"#{topic.replace(' ', '_')} #{random.choice(COUNTRIES).replace(' ', '_')}"
    
    return {
        "text": f"{text} {hashtags}",
        "source": random.choice(["Twitter for iPhone", "Twitter for Android", "Web Client"]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "country": random.choice(COUNTRIES),
        "manual_sentiment": sentiment 
    }

# ---------------------------------------------------------
# 3. التشغيل الرئيسي
# ---------------------------------------------------------
def main():
    producer = create_producer()
    
    # محاولة إعادة الاتصال إذا فشل أول مرة
    while not producer:
        print("🔄 إعادة المحاولة خلال 5 ثوانٍ...")
        time.sleep(5)
        producer = create_producer()

    print(f"🚀 Smart Generator Started... Pumping data to: {KAFKA_TOPIC}")
    print("-------------------------------------------------------")
    
    try:
        while True:
            data = generate_smart_tweet()
            
            # إرسال البيانات
            producer.send(KAFKA_TOPIC, value=data)
            
            # طباعة ملونة جميلة للمراقبة
            icon = "🔴" if data['manual_sentiment'] == "Negative" else "🟢"
            print(f"{icon} [{data['country']}] Sent: {data['text'][:60]}...")
            
            # سرعة متغيرة (بين 0.5 و 1.5 ثانية) لمحاكاة السلوك البشري
            time.sleep(random.uniform(0.5, 1.5)) 
            
    except KeyboardInterrupt:
        print("\n🛑 تم إيقاف المنتج يدوياً.")
        producer.close()

if __name__ == "__main__":
    main()