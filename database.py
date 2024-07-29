import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch

# Database connection
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="red_bus_data",
    user="******",
    password="*****"
)

# Create a cursor
writer = connection.cursor()

# Query to drop the table if it exists and create a new one
table_query = '''
DROP TABLE IF EXISTS bus_routes;
CREATE TABLE bus_routes(
    id SERIAL PRIMARY KEY,
    route_name TEXT NOT NULL,
    route_link TEXT NOT NULL,
    busname TEXT NOT NULL,
    bustype TEXT NOT NULL,
    departing_time TIMESTAMP NOT NULL,
    duration TEXT NOT NULL,
    reaching_time TIMESTAMP NOT NULL,
    star_rating FLOAT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    seats_available INT NOT NULL
)
'''

try:
    # Execute the query to create the table
    writer.execute(table_query)
    connection.commit()
    print("Table created successfully")
except psycopg2.Error as e:
    print(f"Error creating table: {e}")

# Read the CSV file
csv_file = 'bus_details.csv'
data = pd.read_csv(csv_file)

# Prepare the insert query
insert_query = '''
INSERT INTO bus_routes (route_name, route_link, busname, bustype, departing_time, duration, reaching_time, star_rating, price, seats_available)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Insert data into the table
try:
    # Use execute_batch for efficient bulk insertion
    execute_batch(writer, insert_query, data.values)
    connection.commit()
    print("Data inserted successfully")
except psycopg2.Error as e:
    print(f"Error inserting data: {e}")
finally:
    # Close the cursor and connection
    writer.close()
    connection.close()
