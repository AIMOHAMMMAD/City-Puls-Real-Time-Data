import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ==========================================================
# 🌍 إعدادات الاتصال (Infrastructure Config)
# ==========================================================

KAFKA_TOPIC = "social-media-data"
# نستخدم 29092 لأننا نشغل السكريبت من خارج الدوكر (Host Machine)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

# ==========================================================
# 🧠 بنك الكلمات الموسع (Expanded Word Banks)
# ==========================================================
WORDS_UNEMP = [
    "عاطل عن العمل", "بدي وظيفة", "مقابلة عمل", "سيرة ذاتية", "توظيف", 
    "ديوان الخدمة", "سوق العمل", "فرصة عمل", "بطالة مقنعة", "ما في شواغر",
    "تعبت من التقديم", "رفضوني بالشغل", "واسطة", "خريج جديد", "بحث عن عمل",
    "لينكد إن", "HR", "خبرة", "تدريب منتهي بالتوظيف", "عقد عمل", 
    "فترة تجربة", "تسريح موظفين", "القطاع الخاص", "القطاع العام", "مسمى وظيفي",
    "راتب متدني", "شروط تعجيزية", "تخصص راكد", "بستنى التعيين", "دوام جزئي"
]
WORDS_ANXIETY = [
    "قلقان", "توتر", "خايف", "اكتئاب", "مش عارف انام", "تفكير زايد", 
    "ضغط نفسي", "انهيار", "نوبات هلع", "تعبت نفسيا", "مخنوق", "أرق",
    "خوف من المستقبل", "صدمة نفسية", "عزلة", "وسواس", "احتراق وظيفي",
    "Overthinking", "Panic Attack", "مشاعر سلبية", "فقدان شغف", "تشتت انتباه",
    "صداع نصفي", "قولون عصبي", "خيبة أمل", "وحدة", "يأس", "سوداوية"
]
WORDS_ECONOMY = [
    "غلاء المعيشة", "ارتفاع الاسعار", "الضريبة", "البنزين", "الراتب ما بكفي", 
    "الوضع الاقتصادي", "ايجار البيت", "سعر الذهب", "الدولار", "قرض البنك",
    "الديون", "مصاريف", "فقر", "تضخم", "القوة الشرائية", "التعويم",
    "سعر الصرف", "فاتورة الكهرباء", "المواصلات", "ضريبة الدخل", "الرسوم",
    "الاستثمار", "الركود", "الأقساط", "الجمعيات", "مصروف الأولاد"
]
ADJECTIVES = [
    "الوضع كارثي", "صعب جداً", "ما في حل", "الله يعين", "مأساة حقيقية", 
    "اشي بقهر", "تعبنا", "وين المسؤولين", "الوضع سيء", "لا يطاق",
    "وصلنا الحضيض", "حسبي الله", "خربانة", "مش طبيعي الي بصير",
    "سكرت بوجهي", "الأمور بتزداد سوء", "ما عدنا نتحمل"
]
COUNTRIES = ["Jordan", "Saudi Arabia", "Egypt", "UAE", "Lebanon", "Kuwait", "Qatar", "Oman", "Iraq", "Palestine", "Tunisia", "Morocco"]
SOURCES = ["Twitter for iPhone", "Twitter for Android", "Web Client", "Instagram", "LinkedIn"]

# ==========================================================
# 🛠️ دوال النظام
# ==========================================================

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            request_timeout_ms=5000,
            reconnect_backoff_ms=1000
        )
        return producer
    except Exception as e:
        # طباعة الخطأ لمعرفة السبب (مفيد للتصحيح)
        # print(f"⚠️ Connection failed: {e}") 
        return None

def generate_strict_data():
    # اختيار فئة واحدة فقط (بدون خلط)
    category = random.choice(['Unemployment', 'Anxiety', 'Economy'])
    
    if category == 'Unemployment':
        keyword = random.choice(WORDS_UNEMP)
        templates = [
            f"صارلي فترة {keyword} ومسكر بوجهي.. {random.choice(ADJECTIVES)}",
            f"كل ما اقدم {keyword} بيطلبوا خبرة 10 سنين! {random.choice(ADJECTIVES)}",
            f"موضوع الـ {keyword} صار يعتمد ع الواسطة بس."
        ]
        text = random.choice(templates)
        
    elif category == 'Anxiety':
        keyword = random.choice(WORDS_ANXIETY)
        templates = [
            f"حاسس بـ {keyword} مو طبيعي هاليومين.. {random.choice(ADJECTIVES)}",
            f"الـ {keyword} أكل روحي ومش قادر اركز بشغلي.",
            f"تعبت من الـ {keyword} والتفكير المستمر."
        ]
        text = random.choice(templates)
        
    else: # Economy
        keyword = random.choice(WORDS_ECONOMY)
        templates = [
            f"موضوع {keyword} دمر حياتنا.. {random.choice(ADJECTIVES)}",
            f"ارتفاع {keyword} صار خيالي، كيف بدنا نعيش؟",
            f"الراتب طار على {keyword} من اول يوم بالشهر."
        ]
        text = random.choice(templates)

    # إضافة هاشتاجات عشوائية
    hashtags = f"#{keyword.replace(' ', '_')} #{random.choice(COUNTRIES).replace(' ', '_')}"

    return {
        "text": f"{text} {hashtags}",
        "source": random.choice(SOURCES),
        "country": random.choice(COUNTRIES),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiment": "Negative", 
        "manual_label": category
    }

def main():
    print("⏳ Initializing Producer...")
    producer = create_producer()
    
    # حلقة الانتظار حتى يتصل (Wait-and-Retry Logic)
    while not producer:
        print("🔄 Kafka not ready. Retrying in 5 seconds...")
        time.sleep(5)
        producer = create_producer()

    print(f"🚀 Generator Started! Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"🎯 Target Topic: {KAFKA_TOPIC}")
    print("-" * 50)
    
    try:
        while True:
            data = generate_strict_data()
            producer.send(KAFKA_TOPIC, value=data)
            
            # طباعة ملونة لتوضيح البيانات المرسلة
            color_code = "\033[91m" if data['manual_label'] == 'Anxiety' else "\033[94m" if data['manual_label'] == 'Economy' else "\033[93m"
            reset_code = "\033[0m"
            print(f"📤 {color_code}[{data['manual_label']}]{reset_code} Sent: {data['text'][:60]}...")
            
            # سرعة عشوائية للمحاكاة
            time.sleep(random.uniform(0.5, 1.5)) 
            
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()