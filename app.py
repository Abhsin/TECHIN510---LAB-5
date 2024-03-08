import streamlit as st
import pandas.io.sql as sqlio
import altair as alt
import folium
from streamlit_folium import st_folium

from db import conn_str

st.title("Seattle Events")

df = sqlio.read_sql_query("SELECT * FROM events", conn_str)
st.altair_chart(
    alt.Chart(df).mark_bar().encode(x="count()", y=alt.Y("category").sort('-x')).interactive(),
    use_container_width=True,
)

categories = df['category'].unique()
if categories is not None:
    category = st.selectbox("Select a category", categories)
else:
    st.error("No categories found.")

m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)

if categories is not None and category is not None:
    filtered_df = df[df['category'] == category]
    for index, row in filtered_df.iterrows():
        folium.Marker([row['latitude'], row['longitude']], popup=row['event_name']).add_to(m)

st_folium(m, width=1200, height=600)

if categories is not None and category is not None:
    df = df[df['category'] == category]
    st.write(df)

if categories is not None and category is not None:
    filtered_df = df[df['category'] == category]
    chart = alt.Chart(filtered_df).mark_bar().encode(
        x="count()",
        y=alt.Y("category"),
        tooltip=["category", "count()"]
    ).interactive()
    st.altair_chart(chart, use_container_width=True)

st.write("This is a Streamlit app that shows events in Seattle.")