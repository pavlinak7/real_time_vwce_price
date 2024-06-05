import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import time

DB_NAME = 'yahoo'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Create a connection to the database
def get_db_connection():
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)
    return engine


# historic data
@st.cache_data(ttl=60)  # Cache the data for 60 seconds
def fetch_data():
    engine = get_db_connection()
    query = text("SELECT datetime, price FROM vwce ORDER BY datetime")
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

# newest data (=last row)
@st.cache_data(ttl=60)
def fetch_last_row():
    engine = get_db_connection()
    query = text("SELECT datetime, price FROM vwce ORDER BY datetime DESC LIMIT 1")
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df.iloc[0]


# Streamlit app
st.title('VWCE Price Chart')

data = fetch_data()

fig = px.line(data, x='datetime', y='price', markers=True)
fig.update_traces(text=data['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S'), textposition="top center")
fig.update_layout(xaxis_title='Datetime', yaxis_title='Price', hovermode='x unified')
st.plotly_chart(fig)

last_row = fetch_last_row() # Automatically refresh the page when a new row is added

if 'last_datetime' not in st.session_state:
    st.session_state['last_datetime'] = last_row['datetime']

#This line compares the current value of st.session_state['last_datetime'] with last_row['datetime'].
#If they are different, it means that the data has been updated since the last time the code checked.
#It then updates st.session_state['last_datetime'] with the new last_row['datetime'].
#st.rerun() is called to rerun the Streamlit script, which effectively refreshes the page and updates the chart with the latest data.
if st.session_state['last_datetime'] != last_row['datetime']:
    st.session_state['last_datetime'] = last_row['datetime']
    st.rerun()

# Automatically refresh every 60 seconds
time.sleep(60)
st.experimental_rerun()

