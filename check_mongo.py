from pymongo import MongoClient

def check_mongo():
    print("🔄 جاري فحص اتصال MongoDB...")
    try:
        client = MongoClient("mongodb://127.0.0.1:27017/", serverSelectionTimeoutMS=2000)
        client.server_info() # محاولة جلب معلومات السيرفر
        print("✅ اتصال MongoDB: ناجح!")
        
        db_names = client.list_database_names()
        if "citypulse_db" in db_names:
            count = client["citypulse_db"]["sentiment_results"].count_documents({})
            print(f"✅ قاعدة البيانات: موجودة وتحتوي على {count} سجل.")
        else:
            print("ℹ️ قاعدة البيانات: جديدة (سيتم إنشاؤها مع أول رسالة).")
            
    except Exception as e:
        print(f"❌ فشل الاتصال: MongoDB لا يعمل! ({e})")

if __name__ == "__main__":
    check_mongo()