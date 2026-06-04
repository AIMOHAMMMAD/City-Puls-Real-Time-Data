import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import time
import numpy as np
import uuid
from datetime import datetime

# --- 1. System Configuration ---
st.set_page_config(page_title="CityPulse | Enterprise Monitor", layout="wide", page_icon="📡")

st.markdown("""
<style>
    .main { background-color: #0E1117; }
    .stMetric { background-color: #1a1c24; border: 1px solid #333; border-radius: 8px; padding: 10px; border-left: 5px solid #4e73df; }
    h1, h2, h3 { color: #f0f2f6; font-family: 'Segoe UI', sans-serif; }
    .alert-box { padding: 15px; border-radius: 5px; color: #ff6b6b; background-color: #3b1414; border: 1px solid #ff4b4b; text-align: center; font-weight: bold; margin-bottom: 15px; }
    .css-1d391kg { background-color: #1a1c24; }
</style>
""", unsafe_allow_html=True)

# --- 2. Database Layer ---
@st.cache_resource
def init_connection():
    return MongoClient("mongodb://127.0.0.1:27017/")

try:
    client = init_connection()
    db = client["citypulse_db"]
    live_collection = db["sentiment_results"]
    archive_collection = db["historical_archive"]
except: st.error("⚠️ System Offline")

# --- 3. Business Logic ---

# --- قوائم الكلمات المفتاحية الموسعة (للمطابقة مع المنتج) ---

# 1. كلمات البطالة
KEYWORDS_UNEMP = [
    'بطالة', 'وظيفة', 'شغل', 'عمل', 'عاطل', 'راتب', 'توظيف', 'سيرة ذاتية', 'ديوان الخدمة', 
    'سوق العمل', 'فرصة عمل', 'شواغر', 'مقابلة', 'واسطة', 'خريج', 'لينكد', 'hr', 'خبرة', 
    'عقد', 'تسريح', 'قطاع خاص', 'قطاع عام', 'دوام', 'مسمى وظيفي', 'تدريب'
]

# 2. كلمات القلق
KEYWORDS_ANXIETY = [
    'قلق', 'خوف', 'توتر', 'اكتئاب', 'نفسية', 'ضغط', 'أرق', 'تفكير', 'انهيار', 'هلع', 
    'مخنوق', 'صدمة', 'عزلة', 'وسواس', 'احتراق', 'overthinking', 'panic', 'يأس', 'وحدة', 
    'صداع', 'قولون', 'خيبة', 'مشاعر سلبية', 'فقدان شغف'
]

# 3. كلمات الاقتصاد
KEYWORDS_ECONOMY = [
    'غلاء', 'سعر', 'فلوس', 'اقتصاد', 'ضريبة', 'ذهب', 'دولار', 'قرض', 'ديون', 'مصاريف', 
    'فقر', 'تضخم', 'قوة شرائية', 'تعويم', 'صرف', 'فاتورة', 'كهرباء', 'بنزين', 'اقساط', 
    'جمعيات', 'مصروف', 'استثمار', 'ركود', 'ايجار', 'مالية'
]

# --- دالة المنطق المحدثة ---
def detect_topic(text):
    if not isinstance(text, str): return 'General'
    t = text.lower()
    
    # فحص البطالة
    if any(w in t for w in KEYWORDS_UNEMP): return 'Unemployment'
    
    # فحص القلق
    if any(w in t for w in KEYWORDS_ANXIETY): return 'Anxiety'
    
    # فحص الاقتصاد
    if any(w in t for w in KEYWORDS_ECONOMY): return 'Economy'
    
    return 'General'

def map_sentiment(label):
    if not isinstance(label, str): return 'neutral'
    if '5' in label or '4' in label: return 'positive'
    if '1' in label or '2' in label: return 'negative'
    return 'neutral'

COORDS = {"Jordan": [31.95, 35.91], "Saudi Arabia": [24.71, 46.67], "Egypt": [26.82, 30.80], "UAE": [23.42, 53.84], "Lebanon": [33.85, 35.86], "Kuwait": [29.37, 47.97]}
def get_coords(country):
    base = COORDS.get(country, [31.95, 35.91])
    return base[0] + np.random.normal(0, 1.0), base[1] + np.random.normal(0, 1.0)


def forecast_series_scientific(history, steps=10):
    if len(history) < 2: return [history[-1]] * steps
    n_points = min(len(history), 15)
    recent_data = history[-n_points:]
    x = np.arange(len(recent_data))
    y = np.array(recent_data)
    if len(y) > 1: slope, intercept = np.polyfit(x, y, 1)
    else: slope, intercept = 0, y[0]
    future_x = np.arange(len(recent_data), len(recent_data) + steps)
    forecast = slope * future_x + intercept
    return [max(0, int(val)) for val in forecast]

# تهيئة الجلسة
if 'h_unemp' not in st.session_state: st.session_state.h_unemp = [0]*20
if 'h_anx' not in st.session_state: st.session_state.h_anx = [0]*20
if 'h_eco' not in st.session_state: st.session_state.h_eco = [0]*20

# متغيرات التحكم بتحديث الخريطة
if 'last_map_update' not in st.session_state: st.session_state.last_map_update = 0
if 'map_fig' not in st.session_state: st.session_state.map_fig = None

# --- 4. Sidebar ---
with st.sidebar:
    st.title("🎛️ Control Panel")
    st.markdown("---")
    
    selected_view = st.radio(
        "🔍 اختر مؤشر التنبؤ:",
        ["البطالة (Unemployment)", "القلق (Anxiety)", "الاقتصاد (Economy)"],
        index=1
    )
    
    st.markdown("---")
    total_archived = archive_collection.count_documents({})
    st.metric("📦 الأرشيف السنوي", f"{total_archived:,}")
    
    if st.button("📥 Archive & Reset", type="primary"):
        data = list(live_collection.find())
        if data:
            archive_collection.insert_many(data)
            live_collection.delete_many({})
            st.session_state.h_unemp = [0]*20
            st.session_state.h_anx = [0]*20
            st.session_state.h_eco = [0]*20
            st.session_state.map_fig = None 
            st.rerun()

# --- 5. Main Dashboard ---
st.title("📡 CityPulse: Intelligent Situational Awareness")

try:
    # ✅ التعديل هنا: جلب العدد الكلي الحقيقي
    real_total_count = live_collection.count_documents({})
    
    # ✅ التعديل هنا: توسيع الحد إلى 10000 كما طلبت
    cursor = live_collection.find().sort("_id", -1).limit(10000)
    df = pd.DataFrame(list(cursor))
except: df = pd.DataFrame()

if df.empty:
    st.info("⏳ Waiting for data stream..."); time.sleep(2); st.rerun()

# Processing
df.fillna('', inplace=True)
if 'country' not in df.columns: df['country'] = 'Jordan'
df['sentiment'] = df['sentiment_label'].apply(map_sentiment)
df['topic'] = df['text'].apply(detect_topic)

# History Update
unemp_now = len(df[df['topic']=='Unemployment'])
anxiety_now = len(df[df['topic']=='Anxiety'])
economy_now = len(df[df['topic']=='Economy'])

st.session_state.h_unemp.append(unemp_now)
st.session_state.h_anx.append(anxiety_now)
st.session_state.h_eco.append(economy_now)

for h in [st.session_state.h_unemp, st.session_state.h_anx, st.session_state.h_eco]:
    if len(h) > 50: h.pop(0)

# Alert Logic
mean_anx = np.mean(st.session_state.h_anx)
if anxiety_now > mean_anx * 1.5 and anxiety_now > 5:
    st.markdown(f"""<div class="alert-box">🚨 <strong>ANOMALY:</strong> ارتفاع مفاجئ في القلق ({anxiety_now} رسالة)!</div>""", unsafe_allow_html=True)

# KPIs
k1, k2, k3, k4 = st.columns(4)
# ✅ عرض العدد الكلي الحقيقي
k1.metric("Live Volume (Total)", f"{real_total_count:,}") 
k2.metric("📉 Unemployment", unemp_now, delta_color="inverse")
k3.metric("🧠 Anxiety", anxiety_now, delta_color="inverse")
k4.metric("💰 Economy", economy_now, delta_color="inverse")

st.divider()

# Charts Area
col_main1, col_main2 = st.columns([1.5, 1])

with col_main1:
    st.subheader(f"📈 التنبؤ العلمي: {selected_view}")
    
    if "البطالة" in selected_view:
        hist_data = st.session_state.h_unemp
        color = "#e74c3c"
    elif "القلق" in selected_view:
        hist_data = st.session_state.h_anx
        color = "#f39c12"
    else:
        hist_data = st.session_state.h_eco
        color = "#3498db"

    forecast = forecast_series_scientific(hist_data)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=hist_data, mode='lines', name='Actual Data', line=dict(color=color, width=3)))
    x_future = list(range(len(hist_data)-1, len(hist_data)+len(forecast)-1))
    y_future = [hist_data[-1]] + forecast[:-1]
    fig.add_trace(go.Scatter(x=x_future, y=y_future, mode='lines', name='Linear Forecast', line=dict(color='white', width=2, dash='dot')))
    
    fig.update_layout(height=350, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      font=dict(color="white"), xaxis_title="Time Steps", yaxis_title="Volume")
    st.plotly_chart(fig, use_container_width=True, key="static_chart")


with col_main2:
    st.subheader("🌍 مناطق التركيز ()")
    
    current_time = time.time()
    
    if (current_time - st.session_state.last_map_update > 30) or (st.session_state.map_fig is None):
        try:
            df_map = df.copy() 
            
            if not df_map.empty:
                df_map['coords'] = df_map['country'].apply(get_coords)
                df_map['lat'] = df_map['coords'].apply(lambda x: x[0])
                df_map['lon'] = df_map['coords'].apply(lambda x: x[1])
                
                neg_df = df_map[df_map['sentiment'] == 'negative']
                
                fig_map = px.density_mapbox(neg_df, lat='lat', lon='lon', radius=20,
                                            center=dict(lat=25, lon=45), zoom=2.5,
                                            mapbox_style="carto-darkmatter",
                                            color_continuous_scale="reds")
                
                fig_map.update_layout(uirevision='constant', margin={"r":0,"t":0,"l":0,"b":0}, height=350)
                st.session_state.map_fig = fig_map
                st.session_state.last_map_update = current_time
        except: pass

    if st.session_state.map_fig:
        st.plotly_chart(st.session_state.map_fig, use_container_width=True, key="static_map")
    else:
        st.info("جارٍ بناء الخريطة الأولية...")

# Recommendations
st.divider()
c1, c2 = st.columns(2)
with c1:
    if unemp_now > 10: st.error("⚠️ **Labor Ministry:** High unemployment mentions detected.")
    else: st.success("✅ **Labor Ministry:** Stable.")
with c2:
    if anxiety_now > 10: st.warning("⚠️ **Health Ministry:** Anxiety levels rising.")
    else: st.success("✅ **Health Ministry:** Stable.")

time.sleep(3)
st.rerun()