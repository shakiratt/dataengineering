# Import all importables
import http.client
import json
import pandas as pd
import csv
import psycopg2
from psycopg2 import Error
from datetime import datetime
import sys
import requests

#Call API 

# url = "https://api.rentcast.io/v1/properties/random?limit=100"

# headers = {
#     "accept": "application/json",
#     "X-Api-Key": "44db0886c5774e4c9937071c73245d98"
# }

# response = requests.get(url, headers=headers)

# print(response.text)
# response = requests.get(url, headers=headers)
# print(response.json())
# data= response.json()
# #Saving into a file
# filename = "PropertyRecords.json"
# with open(filename,"w") as file:
#     json.dump(data,file,indent=4)

#Read into a dataframe
Propertyrecord_df = pd.read_json('PropertyRecords.json')

Propertyrecord_df['features']=Propertyrecord_df ['features'].apply(json.dumps)

#fill missing nd NA values
Propertyrecord_df.fillna(
    {
        'bathrooms': 0,
        'bedrooms': 0,
        'squareFootage': 0, 
        'county': "Not Available", 
        'propertyType': "unknown",
        'addressLine1': "unknown",
        'city': "unknown", 
        'state': "unknown", 
        'zipCode': "unknown", 
        'formattedAddress': "Not Available",
        'yearBuilt': 0, 
        'features': "unknown", 
        'assessorID': "unknown", 
        'legalDescription': "Not Available",
        'subdivision': "Not Available", 
        'ownerOccupied': 0, 
        'lotSize': 0, 
        'taxAssessments': "Not Available",
        'propertyTaxes': "Not Available", 
        'lastSaleDate': "Not Available", 
        'lastSalePrice': 0, 
        'owner': "unknown", 
        'id': "unknown",
        'longitude': "unknown", 
        'latitude': "unknown", 
        'zoning': "unknown", 
        'addressLine2': "Not Available"
    },
    inplace=True
)


#Create Location Dimension
Location_dim= Propertyrecord_df[['addressLine1','city', 'state','zipCode','county','longitude', 'latitude','addressLine2','subdivision']].drop_duplicates().reset_index(drop=True)
Location_dim.index.name= 'location_id'
Location_dim.to_csv(r'C:\Users\USER\Documents\Data Engineering Projects\Location_dimension.csv', index=True)


#Create Sales Dimension
sales_dim = Propertyrecord_df[['lastSalePrice','lastSaleDate']]
#sales_dim.drop_duplicates(inplace=True)
print(sales_dim)
#sys.exit
sales_dim.index.name= 'sales_id'
sales_dim.to_csv(r'C:\Users\USER\Documents\Data Engineering Projects\sales_dimension.csv', index=False)

#Create Property Features Dimension
features_dim = Propertyrecord_df[['features', 'propertyType', 'zoning','bathrooms', 'bedrooms', 'squareFootage','yearBuilt','ownerOccupied','lotSize']].drop_duplicates().reset_index(drop=True)
features_dim.index.name ='features_id'
features_dim.to_csv(r'C:\Users\USER\Documents\Data Engineering Projects\features_dimension.csv', index=True)


#create the FACT Table
fact_table = Propertyrecord_df['id']
fact_table.index.name= 'fact_id'
fact_table.to_csv(r'C:\Users\USER\Documents\Data Engineering Projects\property_fact.csv', index=False)

#Loading Layer
# develop a function to connect to pgadmin
def get_db_connection():
    connection = psycopg2.connect(
        host= 'localhost',
        database='postgres',
        user='postgres',
        password='Shakirat12'
    )
    return connection
# create Tables
conn = get_db_connection()
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query='''CREATE SCHEMA IF NOT EXISTS zapbank;

                            DROP TABLE IF EXISTS zapbank.fact_table;
                            DROP TABLE IF EXISTS zapbank.location_dim;
                            DROP TABLE IF EXISTS zapbank.sales_dim;
                            DROP TABLE IF EXISTS zapbank.features_dim;

                            
                                CREATE TABLE zapbank.location_dim(
                                    location_id SERIAL PRIMARY KEY,
                                    addressLine1 VARCHAR(255),
                                    city VARCHAR(100),
                                    state VARCHAR(50),
                                    zipCode INTEGER,
                                    county VARCHAR(100),
                                    longitude FLOAT,
                                    latitude FLOAT,
                                    addressLine2 VARCHAR(255),
                                    subdivision VARCHAR(255)
                            
                                );

                                CREATE TABLE zapbank.sales_dim(
                                    sales_id SERIAL PRIMARY KEY,
                                    lastSalePrice FLOAT,
                                    lastSaleDate DATE
                                );

                                    CREATE TABLE zapbank.features_dim(
                                        features_id SERIAL PRIMARY KEY,
                                        features TEXT,
                                        propertyType VARCHAR(255),
                                        zoning VARCHAR(255),
                                        bathrooms FLOAT,
                                        bedrooms FLOAT,
                                        yearBuilt FLOAT,
                                        ownerOccupied VARCHAR(255),
                                        squareFootage FLOAT,
                                        lotSize FLOAT
                                        );



                                    CREATE TABLE zapbank.fact_table(
                                    id VARCHAR (255),
                                    fact_id SERIAL PRIMARY KEY,
                                    sales_id INT,
                                    location_id INT,
                                    features_id INT,
                                    FOREIGN KEY (sales_id) REFERENCES zapbank.sales_dim(sales_id) ON DELETE RESTRICT,
                                    FOREIGN KEY (location_id) REFERENCES zapbank.location_dim(location_id) ON DELETE RESTRICT,
                                    FOREIGN KEY (features_id) REFERENCES zapbank.features_dim(features_id) ON DELETE RESTRICT

                                    );'''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
create_tables()
"""
    Load sales data from CSV to database with proper data type handling and error checking.
    """

def load_data_from_csv_to_sales_table(csv_path, table_name):
    
    try:
        # Get database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Read the CSV file using pandas
        df = pd.read_csv(csv_path)
        print("Original CSV Data:")
        print(df)
        
        # Data type conversions
        if 'lastSalePrice' in df.columns:
            # Convert to float, replacing any non-numeric values with None
            df['lastSalePrice'] = pd.to_numeric(df['lastSalePrice'], errors='coerce')
        
        if 'lastSaleDate' in df.columns:
            # Replace empty strings or 'Not Available' with None
            df['lastSaleDate'] = df['lastSaleDate'].replace(['', 'Not Available'], None)
            # Convert to datetime, invalid parsing becomes None
            df['lastSaleDate'] = pd.to_datetime(df['lastSaleDate'], errors='coerce')
            # Convert timezone-aware timestamps to timezone-naive
            df['lastSaleDate'] = df['lastSaleDate'].dt.tz_localize(None)

        print(df)
        # Iterate over each row and insert into the database
        successful_inserts = 0
        failed_inserts = 0
        db_columns = list(df.columns)
        for index, row in df.iterrows():
            try:
                print(row)
                # Convert row to list and handle None values
                row_data = [None if pd.isna(val) else val for val in row]
                
                # Prepare the insert statement
                placeholders = ', '.join(['%s'] * len(row_data))
                query = f"INSERT INTO {table_name} ({', '.join(db_columns)}) VALUES ({placeholders});"
                
                print(f"\nAttempting insert for row {index + 1}:")
                print(f"Query: {query}")
                print(f"Data: {row_data}")
                
                cursor.execute(query, row_data)
                successful_inserts += 1
                
            except Error as e:
                failed_inserts += 1
                print(f"\nError inserting row {index + 1}:")
                print(f"Data: {row_data}")
                print(f"Error: {str(e)}")
                continue
        
        # Commit the transaction
        conn.commit()
        
        print(f"\nInsert Summary:")
        print(f"Successful inserts: {successful_inserts}")
        print(f"Failed inserts: {failed_inserts}")
        
    except Error as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"General error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



#Create a funcion to load csv data from a folder into the DB
def load_data_from_csv_to_table(csv_path,table_name):
    conn= get_db_connection()
    cursor = conn.cursor()
    with open(csv_path,'r',encoding='utf-8') as file:
        reader=csv.reader(file)
        next(reader) #Skip the header row
        for row in reader:
            placeholders = ', '.join(['%s']* len(row))
            query = f"INSERT INTO {table_name} VALUES ({placeholders});"
            cursor.execute(query,row)
        conn.commit()
        cursor.close()
        conn.close()
# for fact table
fact_csv_path = r'C:\Users\USER\Documents\Data Engineering Projects\property_fact.csv'
load_data_from_csv_to_table(fact_csv_path, 'zapbank.fact_table')
# for Location dimension table
location_csv_path = r'C:\Users\USER\Documents\Data Engineering Projects\Location_dimension.csv'
load_data_from_csv_to_table(location_csv_path, 'zapbank.location_dim')
# for features dimension table
features_csv_path = r'C:\Users\USER\Documents\Data Engineering Projects\features_dimension.csv'
load_data_from_csv_to_table(features_csv_path, 'zapbank.features_dim')

 #Create a New funcion to load csv data for sales  from a folder into the DB
"""def load_data_from_csv_to_sales_table(csv_path,table_name):
    conn= get_db_connection()
    cursor = conn.cursor()
    with open(csv_path,'r',encoding='utf-8') as file:
        reader=csv.reader(file)
        next(reader) #Skip the header row
        for row in reader:
            #convert empty strings (or Not available) in date column to None (NULL in sql)
            row=[None if (cell == '' or cell =='Not Available') and col_name == 'lastSaleDate' else cell for cell, col_name in zip(row, sales_dim_columns)]
            placeholders = ', '.join(['%s']* len(row))
            query = f"INSERT INTO {table_name} VALUES ({placeholders});"
            cursor.execute(query,row)
        conn.commit()
        cursor.close()
        conn.close()

#define the columns names in sales_dim table
sales_dim_columns = ['sales_id','lastSalePrice', 'lastSaleDate']
"""

# for sales dimension table
sales_csv_path = r'C:\Users\USER\Documents\Data Engineering Projects\sales_dimension.csv'
load_data_from_csv_to_sales_table(sales_csv_path, 'zapbank.sales_dim')
print('All data has been loaded successfully into their respective schema and table')

