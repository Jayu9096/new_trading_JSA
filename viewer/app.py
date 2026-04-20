from __future__ import annotations

import os
import pandas as pd
import requests
import streamlit as st


API_BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")

st.set_page_config(page_title="Option Chain Viewer", layout="wide")


def fetch_json(path: str) -> dict:
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is missing")

    response = requests.get(f"{API_BASE_URL}{path}", timeout=15)
    response.raise_for_status()
    return response.json()


st.title("Live Option Chain Viewer")

tab1, tab2 = st.tabs(["NIFTY", "SENSEX"])

with tab1:
    try:
        data = fetch_json("/latest/nifty")
        st.metric("Spot", f"{data.get('spot')}")
        st.metric("PCR", f"{data.get('pcr')}")
        st.caption(f"Timestamp: {data.get('timestamp')}")
        df = pd.DataFrame(data.get("rows", []))
        st.dataframe(df, use_container_width=True, height=650)
    except Exception as exc:
        st.error(f"NIFTY load error: {exc}")

with tab2:
    try:
        data = fetch_json("/latest/sensex")
        st.metric("Spot", f"{data.get('spot')}")
        st.metric("PCR", f"{data.get('pcr')}")
        st.caption(f"Timestamp: {data.get('timestamp')}")
        df = pd.DataFrame(data.get("rows", []))
        st.dataframe(df, use_container_width=True, height=650)
    except Exception as exc:
        st.error(f"SENSEX load error: {exc}")
