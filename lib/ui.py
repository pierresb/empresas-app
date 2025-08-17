# lib/ui.py
import streamlit as st

def inject_global_css():
    st.markdown("""
    <style>
      h1, h2, h3, h4 { letter-spacing: .2px; }
      .stButton>button { border-radius: 8px; padding: .6rem .9rem; font-weight: 600; }
      .stDataFrame { border: 1px solid #E6EBF2; border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)