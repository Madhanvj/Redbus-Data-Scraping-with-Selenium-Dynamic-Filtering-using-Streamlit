import streamlit as st
import pandas as pd
import psycopg2

# Establish connection to PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="red_bus_data",
    user="postgres",
    password="Madhan"
)
writer = connection.cursor()

# Execute initial query and fetch data
writer.execute('SELECT * FROM bus_routes')
data = writer.fetchall()

# Create DataFrame with explicit column names
df = pd.DataFrame(data, columns=['id', 'route_name', 'route_link', 'busname', 
                                 'bustype', 'departing_time', 'duration', 
                                 'reaching_time', 'star_rating', 'price', 
                                 'seats_available'])

st.title("Redbus Data")
st.sidebar.title("Options")

# Sidebar select box for route names
route_names = df['route_name'].unique().tolist()
route_name = st.sidebar.selectbox('Select Route Name', [''] + route_names)

bus_types = ['AC', 'Non AC', 'Others']
bus_type = st.sidebar.selectbox('Select Bus Type', ['', *bus_types])

seat_types = ['Sleeper', 'Semi-Sleeper', 'Seater', 'Others']
seat_type = st.sidebar.selectbox('Select Seat Type', ['', *seat_types])

star_rating = st.sidebar.slider('Select Star Rating', 1.0, 5.0, (1.0, 5.0), 0.1)
price_range=st.sidebar.slider('Select Rate',1,10000,(1,13000),500)
search_button=st.sidebar.button("Search")



# Construct the query with filters
query = "SELECT * FROM bus_routes WHERE 1=1"
if route_name:
    query += f" AND route_name='{route_name}'"
if bus_type:
    if bus_type == 'AC':
        query += " AND (bustype ~* 'AC|A.C|A/C' AND bustype !~* 'Non AC|Non A/C|Non A.C|NON-AC')"
    elif bus_type == 'Non AC':
        query += " AND bustype ~* 'Non AC|Non A/C|Non A.C|NON-AC'"
    elif bus_type == 'Others':
        query += " AND (bustype !~* 'AC|A.C|A/C|Non AC|Non A/C|Non A.C|NON-AC' AND bustype IS NOT NULL)"
if seat_type:
    if seat_type == 'Sleeper':
        query += " AND bustype LIKE '%Sleeper%' AND bustype NOT LIKE '%Semi Sleeper%'"
    elif seat_type == 'Semi-Sleeper':
        query += " AND bustype LIKE '%Semi Sleeper%'"
    elif seat_type == 'Seater':
        query += " AND bustype LIKE '%Seater%'"
    elif seat_type == 'Others':
        query += " AND (bustype NOT LIKE '%Sleeper%' AND bustype NOT LIKE '%Semi Sleeper%' AND bustype NOT LIKE '%Seater%' AND bustype IS NOT NULL)"
if star_rating:
    query += f" AND star_rating >= {star_rating[0]} AND star_rating <= {star_rating[1]}"
if price_range:
    query += f" AND price >= {price_range[0]} AND price <= {price_range[1]}"
# Execute the query
writer.execute(query)
filtered_data = writer.fetchall()

# Create DataFrame with filtered data
filtered_df = pd.DataFrame(filtered_data, columns=['id', 'route_name', 'route_link', 'busname', 
                                                   'bustype', 'departing_time', 'duration', 
                                                   'reaching_time', 'star_rating', 'price', 
                                                   'seats_available'])

st.write(f"Total results: {len(filtered_df)}")
st.write(filtered_df)



# Close the cursor and connection
writer.close()
connection.close()
