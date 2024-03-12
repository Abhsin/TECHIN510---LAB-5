import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import os
from dotenv import load_dotenv
import datetime
from dateutil.relativedelta import relativedelta

# Set page-wide layout
st.set_page_config(layout="wide")

# Load environment variables from .env file
load_dotenv()


# Function to establish PostgreSQL connection
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME')
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Title
st.title("Seattle Events Dashboard")

# Connect to the database
conn = connect_to_postgres()

# Load data from the database
if conn is not None:
    query_all_data = """
        SELECT *
        FROM events
    """
    df_all_data = pd.read_sql_query(query_all_data, conn)

    # Ensure latitude and longitude columns exist
    if 'latitude' not in df_all_data.columns or 'longitude' not in df_all_data.columns:
        st.error("Latitude and/or longitude columns are missing in the database.")
    else:
        # Filter out rows with null latitude values
        df_filtered = df_all_data.dropna(subset=['latitude'])

        # Check if there are any events with latitude data
        if df_filtered.empty:
            st.warning("No events with latitude data found.")
        else:
            # Feature: Map of Events
            st.subheader("Map of Events")
            st.map(df_filtered[['latitude', 'longitude']], use_container_width=True)

            # Feature: Data Visualization

            # Group events by category and count the occurrences
            category_counts = df_filtered['event_type'].value_counts().reset_index()
            category_counts.columns = ['Category', 'EventCount']

            # Chart: Category of events
            st.subheader("What category of events are most common in Seattle?")
            chart = alt.Chart(category_counts).mark_bar().encode(
                x=alt.X('EventCount:Q', title='Number of Events'),
                y=alt.Y('Category:N', title='Event Category', sort='-x')
            ).properties(width=600, height=400)
            st.altair_chart(chart)

            # Group events by month and count the occurrences
            df_filtered['month'] = pd.to_datetime(df_filtered['date_time']).dt.month_name()
            month_counts = df_filtered['month'].value_counts().reset_index()
            month_counts.columns = ['Month', 'EventCount']

            # Chart: Month of events
            st.subheader("What month has the most number of events?")
            chart_month = alt.Chart(month_counts).mark_bar().encode(
                x=alt.X('EventCount:Q', title='Number of Events'),
                y=alt.Y('Month:N', title='Month', sort='-x')
            ).properties(width=600, height=400)
            st.altair_chart(chart_month)

            # Group events by day of the week and count the occurrences
            df_filtered['day_of_week'] = pd.to_datetime(df_filtered['date_time']).dt.day_name()
            day_counts = df_filtered['day_of_week'].value_counts().reset_index()
            day_counts.columns = ['DayOfWeek', 'EventCount']

            # Chart: Day of the week with most events
            st.subheader("What day of the week has the most number of events?")
            chart_day = alt.Chart(day_counts).mark_bar().encode(
                x=alt.X('EventCount:Q', title='Number of Events'),
                y=alt.Y('DayOfWeek:N', title='Day of the Week', sort='-x')
            ).properties(width=600, height=400)
            st.altair_chart(chart_day)

            # Group events by location and count the occurrences
            location_counts = df_filtered['location'].value_counts().reset_index()
            location_counts.columns = ['Location', 'EventCount']

            # Chart: Location of events
            st.subheader("Where are events often held?")
            chart_location = alt.Chart(location_counts).mark_bar().encode(
                x=alt.X('EventCount:Q', title='Number of Events'),
                y=alt.Y('Location:N', title='Event Location', sort='-x')
            ).properties(width=600, height=400)
            st.altair_chart(chart_location)

            # Feature: Data Filtering and Sorting

            # Clear filter button
            if st.button("Clear Filter"):
                filtered_data = df_filtered
            else:
                # Category filter
                selected_category = st.multiselect("Select categories", df_filtered['event_type'].unique())
                filtered_data = df_filtered[df_filtered['event_type'].isin(selected_category)]

            # Display the filtered data in a table
            st.subheader("Filtered Data")
            st.write(filtered_data)

    # Close the database connection
    conn.close()
    st.info("PostgreSQL connection is closed.")
else:
    st.error("Failed to connect to PostgreSQL.")
