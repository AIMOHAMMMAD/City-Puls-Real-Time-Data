import sys
import importlib

def check_library(lib_name):
    try:
        importlib.import_module(lib_name)
        print(f"✅ {lib_name}: مثبتة.")
    except ImportError:
        print(f"❌ {lib_name}: غير مثبتة! (pip install {lib_name})")

print(f"🐍 Python Version: {sys.version.split()[0]}")
libs = ["pyspark", "kafka", "pymongo", "streamlit", "plotly", "pandas", "numpy"]
for lib in libs:
    check_library(lib)