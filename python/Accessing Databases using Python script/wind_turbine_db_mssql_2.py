# import libraries
import pyodbc 
import time
# from datetime import datetime, timedelta
# import pandas as pd
# from string import ascii_lowercase as alc

# Define your connection parameters
SERVER =  'DCC-DE01\DCC' # Change server if need
DATABASE = 'wind_turbine_db' # Change database name that need to create
USERNAME = 'DCC-DE01\DCC'
PASSWORD = ''
TRUSTED = 'no' # for Trusted_Connection if 'yes' SQL Server using the Windows credentials, if 'no' SQL Server Authentication
ENCRYPT = 'no' # for Encrypt

# Create a connection to the database with Windows authentication
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};Trusted_Connection={TRUSTED};Encrypt={ENCRYPT};UID={USERNAME};PWD={PASSWORD};Integrated Security=SSPI;'
conn = pyodbc.connect(connectionString,autocommit=True)
cursor = conn.cursor()

# Switch to using the newly created database
cursor.execute("USE master;")
conn.commit()

# Check if the database exists
check_db_query = f"SELECT name FROM master.dbo.sysdatabases WHERE name = '{DATABASE}'"
cursor.execute(check_db_query)
existing_database = cursor.fetchone()

if existing_database is not None:
    # If the 'existing_database' variable is not None, the database exists
    # Drop the database
    drop_db_query = f"DROP DATABASE {DATABASE};"
    cursor.execute(drop_db_query)
    conn.commit()
    print(f"Database '{DATABASE}' has been dropped.")

# Create a SQL query to create the new database outside of the transaction
create_db_query = f"CREATE DATABASE {DATABASE};"
cursor.execute(create_db_query)


# Switch to using the newly created database
use_db_query = f"USE {DATABASE};"
cursor.execute(use_db_query)
conn.commit()

# Number of turbines
tb=18

# Create status table for plant general data
table_name='status'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        [Number of wind turbines emergency stop] FLOAT,
        [Number of wind turbines pause] FLOAT,
        [Number of wind turbines communication fault] FLOAT,
        [Number of wind turbines in manual mode] FLOAT,
        [Number of wind turbines limited power] FLOAT,
        [Number of wind turbines run and ready] FLOAT
    )
    '''
cursor.execute(query)
conn.commit()

# Create status table for plant general data
table_name='general'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        [Ambient temperature] FLOAT,
        [Wind speed] FLOAT,
        [Wind direction] FLOAT,
        [Total active power] FLOAT,
        [Number of wind turbines online] FLOAT,
        [Capacity factor] FLOAT,
        [Total production] FLOAT,
        [Year production] FLOAT,
        [Month production] FLOAT,
        [Wind availability] FLOAT
    )
    '''
cursor.execute(query)
conn.commit()

# Create status table for plant general data
query = f'''
    CREATE TABLE {table_name} (
        id INT,
        [Number of wind turbines emergency stop] FLOAT,
        [Number of wind turbines pause] FLOAT,
        [Number of wind turbines communication fault] FLOAT,
        [Number of wind turbines in manual mode] FLOAT,
        [Number of wind turbines limited power] FLOAT,
        [Number of wind turbines run and ready] FLOAT,
        [Ambient temperature] FLOAT,
        [Wind speed] FLOAT,
        [Wind direction] FLOAT,
        [Total active power] FLOAT,
        [Number of wind turbines online] FLOAT,
        [Capacity factor] FLOAT,
        [Total production] FLOAT,
        [Year production] FLOAT,
        [Month production] FLOAT,
        [Wind availability] FLOAT
    )
    '''
cursor.execute(query)
conn.commit()

# Create table for each wind turbine data
for i in range(1,tb+1)
    table_name=f'turbine_{i:02}'
    turbine_query = f'''
        CREATE TABLE {table_name} (
            id INT,
            timestamp DATETIME,
            [Active power limit of available stator] FLOAT,
            [Ambient temperature] FLOAT,
            [Auxiliary active power] FLOAT,
            [Auxiliary reactive power]
            [Auxiliary transformer temperature] FLOAT,
            [Average Active Power 15M] FLOAT,
            [Average Ambient Temperature 15M] FLOAT,
            [Average Reactive Power 15M] FLOAT,
            [Average Wind Speed 15M] FLOAT,
            [Converter air temperature] FLOAT,
            [Curr daily production] FLOAT,
            [Curr hourly production] FLOAT,
            [Current month production] FLOAT,
            [Current year production] FLOAT,
            [Filtered main shaft front bearing temperature value] FLOAT,
            [Filtered main shaft rear bearing temperature value] FLOAT,
            [Gearbox bearing temperature] FLOAT,
            [Gearbox oil temperature] FLOAT,
            [Generator 1 winding temperature] FLOAT,
            [Generator 2 winding temperature] FLOAT,
            [Generator 3 winding temperature] FLOAT,
            [Generator bearing temperature, coupling side] FLOAT,
            [Generator bearing temperature, non-coupling side] FLOAT,
            [Generator sliprings temperature] FLOAT,
            [Generator speed (CCU)] FLOAT,
            [Generator speed (filtered PLC)] FLOAT,
            [Grid active power] FLOAT,
            [Grid availability hours current month] FLOAT,
            [Grid Inverter Temperature branch 1] FLOAT,
            [Grid Inverter Temperature branch 2] FLOAT,
            [Grid Inverter Temperature branch 3] FLOAT,
            [Grid phi cosine] FLOAT,
            [Grid reactive power] FLOAT,
            [Hydraulic unit oil temperature] FLOAT,
            [Inverter inductor temperature (rotor) 1st Branch] FLOAT,
            [Inverter inductor temperature (rotor) 2nd Branch] FLOAT,
            [Limit of the maximum active power to generate] FLOAT,
            [Monthly availability %] FLOAT,
            [Nacelle position degrees] FLOAT,
            [Nacelle temperature] FLOAT,
            [Pitch angle] FLOAT,
            [Pitch position of blade 1] FLOAT,
            [Pitch position of blade 2] FLOAT,
            [Pitch position of blade 3] FLOAT,
            [Previous month availability in %] FLOAT,
            [Rotor speed] FLOAT,
            [Total production] FLOAT,
            [Total turbine availability hours] FLOAT,
            [Trafo 1 winding temperature] FLOAT,
            [Trafo 2 winding temperature] FLOAT,
            [Trafo 3 winding temperature] FLOAT,
            [Wind direction] FLOAT,
            [Wind speed] FLOAT,
            [Wind speed 10 min] FLOAT,
            [Wind turbine status] FLOAT      
    )
    '''
    cursor.execute(turbine_query)
    conn.commit()

# Close the cursor and the connection
cursor.close()
conn.close() 