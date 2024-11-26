import requests
import pandas as pd
import psycopg2
import csv
import http.client
import json

# Extraction Layer
url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "78c55a25d1msh6f7408d6ad4204ep179749jsn0864f3fae01e",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

#print(response.json())

data = response.json()

#Save the data to a file
filename = 'PropertyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)

# Read into a dataframe

propertyrecords_df = pd.read_json('PropertyRecords.json')

# Transformation layer
# 1st Step convert distionary column to string
propertyrecords_df['features'] = propertyrecords_df['features'].apply(json.dumps)

#2nd step replace NaN Values with appropriate defaults or remove rows/columns as necessary
propertyrecords_df.fillna({
    'assessorID': 'Unknown',
    'legalDescription': 'Not available',
    'squareFootage': 0,
    'subdivision': 'Not available',
    'yearBuilt': 0,
    'bathrooms': 0,
    'lotSize': 0,
    'propertyType': 'Unknown',
    'lastSalePrice': 0,
    'lastSaleDate': 'Not Available',
    'features': 'None',
    'taxAssessment': 'Not available',
    'owner': 'Unknown',
    'propertyTaxes': 'Not available',
    'bedrooms': 0,
    'ownerOccupied': 0,
    'zoning': 'Unknown',
    'addressLine2': 'Not available',
    'formattedAddress': 'Not available',
    'county': 'Not available'
}, inplace=True)

# Create the fact table
fact_columns = ['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress', 'squareFootage', 'yearBuilt', 'bathrooms', 'bedrooms', 'lotSize', 'propertyType', 'longitude', 'latitude']
fact_table = propertyrecords_df[fact_columns]
fact_table.index.name='property_id'

# Create location Dimension
location_dim = propertyrecords_df[['addressLine1', 'city', 'state', 'zipCode', 'county', 'longitude', 'latitude']].drop_duplicates().reset_index(drop=True)
location_dim.index.name='location_id'

# Create Sales Dimension
sales_dim = propertyrecords_df[['lastSalePrice', 'lastSaleDate']].drop_duplicates().reset_index(drop=True)
sales_dim.index.name = 'sales_id'

# Create Property Features Dimension
features_dim = propertyrecords_df[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
features_dim.index.name = 'features_id'

fact_table.to_csv('property_fact.csv', index=False)
location_dim.to_csv('location_dimension.csv', index=True)
sales_dim.to_csv('sales_dimention.csv', index=True)
features_dim.to_csv('features_dimension.csv', index=True)

# Loading Layer
# develop a function to connect pgadmin
def get_db_connection():
    connection = psycopg.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='School1.'
    )
    return connection
conn = get_db_connection()

# Creating tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''CREATE SCHEMA IF NOT EXISTS zapbank;

                            DROP TABLE IF EXISTS zapbank.fact_table;
                            DROP TABLE IF EXISTS zapbank.location_dim;
                            DROP TABLE IF EXISTS zapbank.sales_dim;
                            DROP TABLE IF EXISTS zapbank.features_dim;

                            CREATE TABLE zapbank.fact_table(
                                property_id SERIAL PRIMARY KEY,
                                addressLine1 VARCHAR(255),
                                city VARCHAR(100),
                                state VARCHAR(100),
                                zipCode INTEGER,
                                formattedAddress VARCHAR(255),
                                squareFootage FLOAT,
                                yearBuilt FLOAT,
                                bathrooms FLOAT,
                                bedrooms FLOAT,
                                lotSize FLOAT,
                                propertyType VARCHAR(100),
                                longitude FLOAT,
                                latitude FLOAT
                            );

                            CREATE TABLE zapbank.location_dim(
                                location_id SERIAL PRIMARY KEY,
                                addressLine1 VARCHAR(255),
                                city VARCHAR(100),
                                state VARCHAR(100),
                                zipCode INTEGER,
                                county VARCHAR(100),
                                longitude FLOAT,
                                latitude FLOAT
                            );

                            CREATE TABLE zapbank.sales_dim(
                                sales_id SERIAL PRIMARY KEY,
                                lastSalePrice FLOAT,
                                lastSaleDate DATE
                            );

                            CREATE TABLE zapbank.features_dim(
                                features_id SERIAL PRIMARY KEY,
                                features TEXT,
                                propertyType VARCHAR(100),
                                zoning VARCHAR(100)
                            );'''
    
    cursor.execute(create_table_query)
    conn.commit()
    conn.rollback()
    cursor.close()
    conn.close()

create_tables()

# Function to load CSV data into a table
def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Open the CSV file
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Skip the header row

            # Dynamically create placeholders for the values
            num_columns = len(headers)
            placeholders = ', '.join(['%s'] * num_columns)
            query = f'INSERT INTO {table_name} VALUES ({placeholders})'

            # Insert each row into the table
            for row in reader:
                cursor.execute(query, row)
        
        conn.commit()
        print(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Fact table path
fact_csv_path = r'C:\Users\olalekan\Downloads\Data Engineering\Amdari\Projects -Real Estate Data Evolution Streamlining Property Records Management with an Efficient PostgresSQL ETL Pipeline for Zipco\property_fact.csv'

# Load data into the fact_table
load_data_from_csv_to_table(fact_csv_path, 'zapbank.fact_table')


# Location dimension table CSV path
location_csv_path = r'C:\Users\olalekan\Downloads\Data Engineering\Amdari\Projects -Real Estate Data Evolution Streamlining Property Records Management with an Efficient PostgresSQL ETL Pipeline for Zipco\location_dimension.csv'

# Load data into location_dim table
load_data_from_csv_to_table(location_csv_path, 'zapbank.location_dim')


# Features dimension table CSV path
features_csv_path = r'C:\Users\olalekan\Downloads\Data Engineering\Amdari\Projects -Real Estate Data Evolution Streamlining Property Records Management with an Efficient PostgresSQL ETL Pipeline for Zipco\API to Postgres\features_dimension.csv'

# Load data into features_dim table
load_data_from_csv_to_table(features_csv_path, 'zapbank.features_dim')


# Function to load data into the sales table
def load_data_from_csv_to_sales_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Define the sales_dim column names
    sale_dim_columns = ['sales_id', 'lastSalePrice', 'lastSaleDate']

    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Skip the header row

            # Ensure CSV columns align with the table schema
            if headers != sale_dim_columns:
                raise ValueError("CSV headers do not match the expected table columns!")

            for row in reader:
                # Convert empty strings or 'Not available' in date column to None
                row = [
                    None if (cell == '' or cell.lower() == 'not available') and col_name == 'lastSaleDate' 
                    else cell 
                    for cell, col_name in zip(row, sale_dim_columns)
                ]

                # Create placeholders dynamically based on row length
                placeholders = ', '.join(['%s'] * len(row))
                query = f'INSERT INTO {table_name} VALUES ({placeholders})'
                cursor.execute(query, row)

        conn.commit()
        print(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Sales dimension table CSV path
sales_csv_path = r'C:\Users\olalekan\Downloads\Data Engineering\Amdari\Projects -Real Estate Data Evolution Streamlining Property Records Management with an Efficient PostgresSQL ETL Pipeline for Zipco\API to Postgres\sales_dimention.csv'

# Load data into sales_dim table
load_data_from_csv_to_sales_table(sales_csv_path, 'zapbank.sales_dim')


print('All Data has been loaded successfully into their respective schema and tables')
