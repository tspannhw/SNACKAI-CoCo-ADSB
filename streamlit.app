import streamlit as st
import pandas as pd
import altair as alt
import pydeck as pdk
import json
from snowflake.snowpark.context import get_active_session
import _snowflake

st.set_page_config(
    page_title="ADS-B Aircraft Dashboard",
    page_icon="✈️",
    layout="wide"
)

@st.cache_data(ttl=300)
def load_data(_session, filters):
    query = """
        SELECT 
            FLIGHT, REGISTRATION, AIRCRAFT_TYPE, CATEGORY, DESCRIPTION,
            ALTITUDE_BARO, GROUND_SPEED, VERTICAL_RATE, TRACK,
            LATITUDE, LONGITUDE, SQUAWK, RSSI,
            DATETIMESTAMP, ICAO_HEX
        FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
        WHERE DATETIMESTAMP IS NOT NULL
    """
    if filters.get("aircraft_type"):
        query += f" AND AIRCRAFT_TYPE = '{filters['aircraft_type']}'"
    if filters.get("min_altitude"):
        query += f" AND ALTITUDE_BARO >= {filters['min_altitude']}"
    if filters.get("max_altitude"):
        query += f" AND ALTITUDE_BARO <= {filters['max_altitude']}"
    if filters.get("category"):
        query += f" AND CATEGORY = '{filters['category']}'"
    query += " ORDER BY DATETIMESTAMP DESC LIMIT 5000"
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def get_aircraft_types(_session):
    return _session.sql("""
        SELECT DISTINCT AIRCRAFT_TYPE 
        FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA 
        WHERE AIRCRAFT_TYPE IS NOT NULL 
        ORDER BY AIRCRAFT_TYPE
    """).to_pandas()["AIRCRAFT_TYPE"].tolist()

@st.cache_data(ttl=600)
def get_categories(_session):
    return _session.sql("""
        SELECT DISTINCT CATEGORY 
        FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA 
        WHERE CATEGORY IS NOT NULL 
        ORDER BY CATEGORY
    """).to_pandas()["CATEGORY"].tolist()

def send_analyst_message(session, messages):
    request_body = {
        "messages": messages,
        "semantic_view": "DEMO.DEMO.SVADSBAIRCRAFT",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(f"Failed request with status {resp['status']}: {resp['content']}")

session = get_active_session()

st.title("✈️ ADS-B Aircraft Dashboard")

with st.sidebar:
    st.header("Filters")
    
    aircraft_types = get_aircraft_types(session)
    selected_type = st.selectbox(
        "Aircraft Type",
        options=["All"] + aircraft_types,
        index=0
    )
    
    categories = get_categories(session)
    selected_category = st.selectbox(
        "Category",
        options=["All"] + categories,
        index=0
    )
    
    col1, col2 = st.columns(2)
    with col1:
        min_alt = st.number_input("Min Altitude (ft)", value=0, min_value=0, step=1000)
    with col2:
        max_alt = st.number_input("Max Altitude (ft)", value=50000, min_value=0, step=1000)
    
    if st.button("Refresh Data", type="primary"):
        st.cache_data.clear()
    
    st.divider()
    st.header("Export Options")
    export_format = st.radio("Export Format", ["CSV", "JSON", "Excel"], horizontal=True)

filters = {
    "aircraft_type": selected_type if selected_type != "All" else None,
    "category": selected_category if selected_category != "All" else None,
    "min_altitude": min_alt,
    "max_altitude": max_alt
}

df = load_data(session, filters)

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Aircraft", len(df["ICAO_HEX"].unique()))
with col2:
    st.metric("Avg Altitude", f"{df['ALTITUDE_BARO'].mean():,.0f} ft" if not df.empty else "N/A")
with col3:
    st.metric("Avg Speed", f"{df['GROUND_SPEED'].mean():,.0f} kts" if not df.empty else "N/A")
with col4:
    st.metric("Max Altitude", f"{df['ALTITUDE_BARO'].max():,.0f} ft" if not df.empty else "N/A")
with col5:
    st.metric("Records", f"{len(df):,}")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Data Table", "Map View", "Charts", "Category Analysis", "Time Analysis", "Chat with Data"
])

with tab1:
    st.subheader("Aircraft Data")
    st.dataframe(
        df[["FLIGHT", "REGISTRATION", "AIRCRAFT_TYPE", "CATEGORY", "ALTITUDE_BARO", 
            "GROUND_SPEED", "TRACK", "LATITUDE", "LONGITUDE", "DATETIMESTAMP"]],
        column_config={
            "ALTITUDE_BARO": st.column_config.NumberColumn("Altitude (ft)", format="%d"),
            "GROUND_SPEED": st.column_config.NumberColumn("Speed (kts)", format="%d"),
            "TRACK": st.column_config.NumberColumn("Track (°)", format="%d"),
            "LATITUDE": st.column_config.NumberColumn("Lat", format="%.4f"),
            "LONGITUDE": st.column_config.NumberColumn("Lon", format="%.4f"),
        },
        use_container_width=True,
        hide_index=True
    )
    
    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name="adsb_aircraft_data.csv",
            mime="text/csv"
        )
    with col2:
        json_data = df.to_json(orient="records", indent=2)
        st.download_button(
            label="Download JSON",
            data=json_data,
            file_name="adsb_aircraft_data.json",
            mime="application/json"
        )
    with col3:
        try:
            import io
            buffer = io.BytesIO()
            df.to_excel(buffer, index=False, engine='openpyxl')
            st.download_button(
                label="Download Excel",
                data=buffer.getvalue(),
                file_name="adsb_aircraft_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except ImportError:
            st.info("Excel export requires openpyxl library")

with tab2:
    st.subheader("Aircraft Locations")
    map_data = df[df["LATITUDE"].notna() & df["LONGITUDE"].notna()].copy()
    if not map_data.empty:
        map_data = map_data.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})
        center_lat = map_data["lat"].mean()
        center_lon = map_data["lon"].mean()
        
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=4,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_data,
                    get_position="[lon, lat]",
                    get_color="[0, 128, 255, 160]",
                    get_radius=5000,
                    pickable=True,
                ),
            ],
            tooltip={"text": "{FLIGHT}\n{AIRCRAFT_TYPE}\nAlt: {ALTITUDE_BARO} ft"}
        ))
        
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Aircraft on Map", len(map_data["ICAO_HEX"].unique()))
        with col2:
            st.metric("Data Points", len(map_data))
    else:
        st.info("No location data available")

with tab3:
    st.subheader("Aircraft Visualizations")
    
    if not df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container(border=True):
                st.markdown("**Altitude Distribution by Aircraft Type**")
                alt_data = df[df["ALTITUDE_BARO"].notna()].copy()
                if not alt_data.empty:
                    chart = alt.Chart(alt_data).mark_bar().encode(
                        x=alt.X("AIRCRAFT_TYPE:N", title="Aircraft Type", sort="-y"),
                        y=alt.Y("mean(ALTITUDE_BARO):Q", title="Avg Altitude (ft)"),
                        color=alt.Color("AIRCRAFT_TYPE:N", legend=None),
                        tooltip=["AIRCRAFT_TYPE", "mean(ALTITUDE_BARO):Q"]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
        
        with col2:
            with st.container(border=True):
                st.markdown("**Speed vs Altitude**")
                scatter_data = df[(df["GROUND_SPEED"].notna()) & (df["ALTITUDE_BARO"].notna())].copy()
                if not scatter_data.empty:
                    chart = alt.Chart(scatter_data.head(500)).mark_circle(size=60).encode(
                        x=alt.X("GROUND_SPEED:Q", title="Ground Speed (kts)"),
                        y=alt.Y("ALTITUDE_BARO:Q", title="Altitude (ft)"),
                        color=alt.Color("CATEGORY:N", title="Category"),
                        tooltip=["FLIGHT", "AIRCRAFT_TYPE", "GROUND_SPEED", "ALTITUDE_BARO"]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
        
        col3, col4 = st.columns(2)
        
        with col3:
            with st.container(border=True):
                st.markdown("**Aircraft Count by Category**")
                cat_counts = df.groupby("CATEGORY")["ICAO_HEX"].nunique().reset_index()
                cat_counts.columns = ["Category", "Count"]
                chart = alt.Chart(cat_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Category:N"),
                    tooltip=["Category", "Count"]
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
        
        with col4:
            with st.container(border=True):
                st.markdown("**Speed Distribution**")
                speed_data = df[df["GROUND_SPEED"].notna()].copy()
                if not speed_data.empty:
                    chart = alt.Chart(speed_data).mark_bar().encode(
                        x=alt.X("GROUND_SPEED:Q", bin=alt.Bin(maxbins=20), title="Ground Speed (kts)"),
                        y=alt.Y("count():Q", title="Count"),
                        tooltip=["count()"]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data available for charts")

with tab4:
    st.subheader("Category Analysis")
    
    if not df.empty:
        category_stats = df.groupby("CATEGORY").agg({
            "ICAO_HEX": "nunique",
            "ALTITUDE_BARO": ["mean", "max", "min"],
            "GROUND_SPEED": ["mean", "max"]
        }).round(0)
        category_stats.columns = ["Aircraft Count", "Avg Altitude", "Max Altitude", "Min Altitude", "Avg Speed", "Max Speed"]
        category_stats = category_stats.reset_index()
        
        st.dataframe(
            category_stats,
            column_config={
                "Aircraft Count": st.column_config.NumberColumn(format="%d"),
                "Avg Altitude": st.column_config.NumberColumn(format="%d ft"),
                "Max Altitude": st.column_config.NumberColumn(format="%d ft"),
                "Min Altitude": st.column_config.NumberColumn(format="%d ft"),
                "Avg Speed": st.column_config.NumberColumn(format="%d kts"),
                "Max Speed": st.column_config.NumberColumn(format="%d kts"),
            },
            use_container_width=True,
            hide_index=True
        )
        
        st.divider()
        
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown("**Altitude by Category (Box Plot)**")
                box_data = df[df["ALTITUDE_BARO"].notna()].copy()
                if not box_data.empty:
                    chart = alt.Chart(box_data).mark_boxplot(extent='min-max').encode(
                        x=alt.X("CATEGORY:N", title="Category"),
                        y=alt.Y("ALTITUDE_BARO:Q", title="Altitude (ft)"),
                        color=alt.Color("CATEGORY:N", legend=None)
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
        
        with col2:
            with st.container(border=True):
                st.markdown("**Speed by Category**")
                speed_cat = df[df["GROUND_SPEED"].notna()].groupby("CATEGORY")["GROUND_SPEED"].mean().reset_index()
                chart = alt.Chart(speed_cat).mark_bar().encode(
                    x=alt.X("CATEGORY:N", title="Category", sort="-y"),
                    y=alt.Y("GROUND_SPEED:Q", title="Avg Speed (kts)"),
                    color=alt.Color("CATEGORY:N", legend=None)
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data available for category analysis")

with tab5:
    st.subheader("Time-Based Analysis")
    
    if not df.empty and "DATETIMESTAMP" in df.columns:
        time_df = df.copy()
        time_df["DATETIMESTAMP"] = pd.to_datetime(time_df["DATETIMESTAMP"])
        time_df["hour"] = time_df["DATETIMESTAMP"].dt.hour
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container(border=True):
                st.markdown("**Aircraft Activity by Hour**")
                hourly = time_df.groupby("hour")["ICAO_HEX"].nunique().reset_index()
                hourly.columns = ["Hour", "Aircraft Count"]
                chart = alt.Chart(hourly).mark_line(point=True).encode(
                    x=alt.X("Hour:O", title="Hour of Day"),
                    y=alt.Y("Aircraft Count:Q", title="Unique Aircraft"),
                    tooltip=["Hour", "Aircraft Count"]
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
        
        with col2:
            with st.container(border=True):
                st.markdown("**Speed Over Time**")
                speed_time = time_df[time_df["GROUND_SPEED"].notna()].set_index("DATETIMESTAMP")["GROUND_SPEED"].resample("5min").mean().reset_index()
                speed_time.columns = ["Time", "Avg Speed"]
                if not speed_time.empty:
                    chart = alt.Chart(speed_time).mark_area(opacity=0.6).encode(
                        x=alt.X("Time:T", title="Time"),
                        y=alt.Y("Avg Speed:Q", title="Avg Speed (kts)"),
                        tooltip=["Time:T", "Avg Speed:Q"]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
        
        with st.container(border=True):
            st.markdown("**Altitude Trends Over Time**")
            alt_time = time_df[time_df["ALTITUDE_BARO"].notna()].set_index("DATETIMESTAMP")["ALTITUDE_BARO"].resample("5min").agg(["mean", "max", "min"]).reset_index()
            alt_time.columns = ["Time", "Avg", "Max", "Min"]
            if not alt_time.empty:
                base = alt.Chart(alt_time).encode(x=alt.X("Time:T", title="Time"))
                area = base.mark_area(opacity=0.3).encode(
                    y=alt.Y("Min:Q", title="Altitude (ft)"),
                    y2="Max:Q"
                )
                line = base.mark_line(color="blue").encode(y="Avg:Q")
                st.altair_chart((area + line).properties(height=250), use_container_width=True)
    else:
        st.info("No time data available")

with tab6:
    st.subheader("Chat with Aircraft Data")
    st.caption("Ask questions about aircraft data using natural language (powered by Cortex Analyst)")
    
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    
    for msg in st.session_state.chat_messages:
        role = "assistant" if msg["role"] == "analyst" else msg["role"]
        with st.chat_message(role):
            for item in msg.get("content", []):
                if item["type"] == "text":
                    st.markdown(item["text"])
                elif item["type"] == "sql":
                    with st.expander("SQL Query", expanded=False):
                        st.code(item["statement"], language="sql")
                    with st.expander("Results", expanded=True):
                        try:
                            result_df = session.sql(item["statement"]).to_pandas()
                            st.dataframe(result_df, use_container_width=True, hide_index=True)
                            if len(result_df) > 1 and len(result_df.columns) >= 2:
                                st.line_chart(result_df.set_index(result_df.columns[0]))
                        except Exception as e:
                            st.error(f"Error executing SQL: {e}")
                elif item["type"] == "suggestions":
                    st.info("Suggestions: " + ", ".join(item.get("suggestions", [])))
    
    if prompt := st.chat_input("Ask about aircraft data (e.g., 'How many aircraft are flying above 30000 feet?')"):
        st.session_state.chat_messages.append({
            "role": "user",
            "content": [{"type": "text", "text": prompt}]
        })
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing your question..."):
                try:
                    api_messages = [{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_messages]
                    response = send_analyst_message(session, api_messages)
                    content = response.get("message", {}).get("content", [])
                    
                    st.session_state.chat_messages.append({
                        "role": "analyst",
                        "content": content
                    })
                    
                    for item in content:
                        if item["type"] == "text":
                            st.markdown(item["text"])
                        elif item["type"] == "sql":
                            with st.expander("SQL Query", expanded=False):
                                st.code(item["statement"], language="sql")
                            with st.expander("Results", expanded=True):
                                try:
                                    result_df = session.sql(item["statement"]).to_pandas()
                                    st.dataframe(result_df, use_container_width=True, hide_index=True)
                                    if len(result_df) > 1 and len(result_df.columns) >= 2:
                                        st.line_chart(result_df.set_index(result_df.columns[0]))
                                except Exception as e:
                                    st.error(f"Error executing SQL: {e}")
                        elif item["type"] == "suggestions":
                            st.info("Suggestions: " + ", ".join(item.get("suggestions", [])))
                except Exception as e:
                    st.error(f"Error communicating with Cortex Analyst: {e}")
    
    if st.button("Clear Chat History"):
        st.session_state.chat_messages = []
        st.rerun()
