# realtime_dashboard.py - النسخة النهائية المتكاملة (MongoDB + UI المتقدمة)

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from collections import deque, Counter
from datetime import datetime, timedelta
import time
import random
import numpy as np
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from pymongo import MongoClient
import warnings

warnings.filterwarnings('ignore')

# إعدادات الصفحة
st.set_page_config(
    page_title="نبض المدينة - نظام البث المباشر",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# --------------------------------------------------------
# 🔌 الاتصال بـ MongoDB (البيانات الحقيقية)
# --------------------------------------------------------
@st.cache_resource
def init_mongo_connection():
    try:
        # الاتصال بـ MongoDB عبر المنفذ الخارجي لـ Docker
        client = MongoClient("mongodb://127.0.0.1:27017/")
        return client
    except Exception as e:
        st.error(f"❌ خطأ في الاتصال بـ MongoDB: {e}")
        return None

try:
    client = init_mongo_connection()
    db = client["citypulse"]
    collection = db["mental_enriched_nlp"]
except:
    st.error("⚠️ يرجى التأكد من تشغيل Docker")

# --------------------------------------------------------
# 🧠 تهيئة الحالة والتحليل
# --------------------------------------------------------

if 'messages' not in st.session_state:
    st.session_state.messages = deque(maxlen=500)

if 'metrics' not in st.session_state:
    st.session_state.metrics = {
        'total_messages': 0,
        'positive_count': 0,
        'negative_count': 0,
        'neutral_count': 0,
        'avg_confidence': 0.85, 
        'messages_per_minute': 0,
        'peak_activity': 0,
        'streaming_active': True
    }

# دوال التحليل المتقدمة (Client-Side Analytics)
def get_arabic_stopwords():
    return set(['في', 'من', 'الى', 'على', 'أن', 'إن', 'ما', 'هذا', 'و', 'أو', 'عن', 'مع', 'كان', 'انا'])

def preprocess_arabic_text(text):
    if not text: return ""
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\d+', '', text)
    return text.strip()

def extract_keywords_arabic(text, num_keywords=5):
    if not text: return []
    words = preprocess_arabic_text(text).split()
    stopwords = get_arabic_stopwords()
    meaningful_words = [w for w in words if w not in stopwords and len(w) > 2]
    return list(set(meaningful_words))[:num_keywords]

def analyze_message_categories(text):
    categories_map = {
        'اقتصادية': ['سعر', 'غلاء', 'راتب', 'فلوس', 'مال', 'سوق', 'تكلفة'],
        'صحية': ['مرض', 'علاج', 'مستشفى', 'دواء', 'صحة', 'تعب', 'طبيب'],
        'اجتماعية': ['عائلة', 'زواج', 'أصدقاء', 'ناس', 'مجتمع', 'زيارة'],
        'عمل': ['وظيفة', 'شغل', 'مدير', 'دوام', 'شركة', 'مشروع'],
        'عامة': []
    }
    for cat, keywords in categories_map.items():
        if any(k in text for k in keywords):
            return cat
    return 'عامة'

# --------------------------------------------------------
# 🔄 جلب البيانات الحقيقية
# --------------------------------------------------------

def fetch_data_from_mongo():
    """جلب أحدث البيانات من MongoDB وتحديث الحالة"""
    if not st.session_state.metrics['streaming_active']:
        return

    try:
        # جلب أحدث 50 رسالة
        cursor = collection.find().sort("_id", -1).limit(50)
        new_docs = list(cursor)
        
        # نعكس الترتيب لتدخل الأقدم أولاً في الـ deque
        new_docs.reverse()

        for doc in new_docs:
            doc_id = str(doc.get('_id'))
            
            # تجنب التكرار
            if any(msg.get('id') == doc_id for msg in st.session_state.messages):
                continue
            
            text = doc.get('text', '')
            sentiment = doc.get('sentiment', 'neutral')
            
            # تطبيق التحليلات المتقدمة محلياً (لأن Spark يرسل النص والمشاعر فقط)
            keywords = extract_keywords_arabic(text)
            category = analyze_message_categories(text)
            
            # إثراء البيانات للعرض
            msg_obj = {
                'id': doc_id,
                'text': text,
                'sentiment': sentiment,
                'category': category,
                'confidence': 0.85 + random.uniform(-0.05, 0.05), # محاكاة ثقة بسيطة للعرض
                'city': doc.get('country', 'غير محدد'),
                'age': doc.get('age', 0),
                'gender': doc.get('gender', 'غير محدد'),
                'timestamp': datetime.now(), # وقت الوصول للوحة
                'positive_words': len(keywords) if sentiment == 'positive' else 0,
                'negative_words': len(keywords) if sentiment == 'negative' else 0,
                'keywords': keywords,
                'auto_categories': [category],
                'analysis_engine': 'Spark NLP'
            }
            
            st.session_state.messages.append(msg_obj)
        
        update_metrics()
        
    except Exception as e:
        pass

def update_metrics():
    msgs = list(st.session_state.messages)
    if not msgs: return

    metrics = st.session_state.metrics
    metrics['total_messages'] = len(msgs)
    metrics['positive_count'] = sum(1 for m in msgs if m['sentiment'] == 'positive')
    metrics['negative_count'] = sum(1 for m in msgs if m['sentiment'] == 'negative')
    metrics['messages_per_minute'] = len(msgs) # تبسيط

# --------------------------------------------------------
# 📊 واجهة المستخدم (مأخوذة من الملف المتقدم)
# --------------------------------------------------------

def display_header():
    status = "🟢 متصل (Spark Streaming)" if st.session_state.metrics['streaming_active'] else "🔴 متوقف"
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 2rem; border-radius: 15px; color: white; text-align: center; margin-bottom: 2rem;">
        <h1 style="margin:0;">📊 نبض المدينة - البث الحي</h1>
        <p style="opacity:0.8;">مراقبة وتحليل البيانات الحقيقية من Kafka & Spark</p>
        <div style="margin-top:10px;">
            <span style="background:rgba(255,255,255,0.2); padding:5px 15px; border-radius:20px;">{status}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def display_metrics_row():
    m = st.session_state.metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("إجمالي الرسائل", m['total_messages'])
    c2.metric("😊 إيجابية", m['positive_count'])
    c3.metric("😔 سلبية", m['negative_count'])
    ratio = (m['positive_count'] / m['total_messages'] * 100) if m['total_messages'] > 0 else 0
    c4.metric("مؤشر الإيجابية", f"{ratio:.1f}%")

def display_charts():
    if len(st.session_state.messages) < 1:
        st.info("⏳ في انتظار وصول البيانات من Spark...")
        return

    df = pd.DataFrame(st.session_state.messages)
    
    tab1, tab2, tab3 = st.tabs(["📊 المشاعر", "🌍 المدن", "📈 الاتجاهات"])
    
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.pie(df, names='sentiment', title='توزيع المشاعر', 
                         color='sentiment',
                         color_discrete_map={'positive':'#2ecc71', 'negative':'#e74c3c', 'neutral':'#95a5a6'})
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig2 = px.bar(df, x='category', color='sentiment', title='المشاعر حسب الفئة',
                          color_discrete_map={'positive':'#2ecc71', 'negative':'#e74c3c', 'neutral':'#95a5a6'})
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        fig_map = px.histogram(df, x='city', color='sentiment', barmode='group', title='توزيع الرسائل حسب المدينة',
                               color_discrete_map={'positive':'#2ecc71', 'negative':'#e74c3c', 'neutral':'#95a5a6'})
        st.plotly_chart(fig_map, use_container_width=True)
    
    with tab3:
        if len(df) > 5:
            df['time_str'] = df['timestamp'].dt.strftime('%H:%M:%S')
            fig_line = px.line(df, x='time_str', y=df.index, title='تدفق الرسائل')
            st.plotly_chart(fig_line, use_container_width=True)

def display_live_feed():
    st.subheader("💬 شريط الأخبار العاجلة")
    msgs = list(st.session_state.messages)[-5:]
    
    for msg in reversed(msgs):
        color = "#28a745" if msg['sentiment'] == 'positive' else "#dc3545" if msg['sentiment'] == 'negative' else "#6c757d"
        icon = "😊" if msg['sentiment'] == 'positive' else "😔" if msg['sentiment'] == 'negative' else "😐"
        
        st.markdown(f"""
        <div style="border-right: 5px solid {color}; background: rgba(255,255,255,0.05); padding: 15px; margin-bottom: 10px; border-radius: 5px; border: 1px solid #ddd;">
            <div style="font-size: 1.1em; font-weight: bold;">{icon} {msg['text']}</div>
            <div style="font-size: 0.8em; color: gray; margin-top: 5px;">
                📍 {msg['city']} | 👤 {msg['gender']} ({msg['age']}) | 🏷️ {msg['category']} | 🚀 Spark NLP
            </div>
        </div>
        """, unsafe_allow_html=True)

def main():
    display_header()
    
    # 🔄 جلب البيانات الحقيقية من Mongo
    fetch_data_from_mongo()
    
    display_metrics_row()
    st.markdown("---")
    display_charts()
    st.markdown("---")
    display_live_feed()
    
    # تحديث تلقائي
    if st.session_state.metrics['streaming_active']:
        time.sleep(2)
        st.rerun()

if __name__ == "__main__":
    main()