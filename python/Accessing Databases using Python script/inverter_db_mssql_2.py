# import libraries
import pyodbc 
import time
# from datetime import datetime, timedelta
# import pandas as pd
# from string import ascii_lowercase as alc

# Define your connection parameters
SERVER =  'DCC-DE01\DCC' # Change server if need
DATABASE = 'inverter_db' # Change database name that need to create
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

# Number of inverters in every single compact station
cs={1:65, 2:66, 3:66, 4:66, 5:65, 6:44, 7:42, 8:65, 9:20}

# Create status table
table_name = 'status'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        int_id VARCHAR(6) NOT NULL,
        cs_id VARCHAR(4) NOT NULL,
        [Abnormal Equipment] BIT DEFAULT 1,
        [Abnormal Ground] BIT DEFAULT 1,
        [Abnormal Leakage Current] BIT DEFAULT 1,
        [Abnormal Monitor Unit] BIT DEFAULT 1,
        [Abnormal String] BIT DEFAULT 1,
        [AFCI Self-test Fault] BIT DEFAULT 1,
        [COMUNICATION STATUS] BIT DEFAULT 1,
        [DC Arc Fault] BIT DEFAULT 1,
        [Grid Frequency Instability] BIT DEFAULT 1,
        [Grid Overfrequency] BIT DEFAULT 1,
        [Grid Underfrequency] BIT DEFAULT 1,
        [Grid Overvoltage] BIT DEFAULT 1,
        [Grid Undervoltage] BIT DEFAULT 1,
        [High String Voltage] BIT DEFAULT 1,
        [High String Voltage to Ground] BIT DEFAULT 1,
        [High Temperature] BIT DEFAULT 1,
        [INVERTER LOCKED] BIT DEFAULT 1,
        [INVERTER STATUS] BIT DEFAULT 1,
        [INVERTER WARNING] BIT DEFAULT 1,
        [Large DC of Output current] BIT DEFAULT 1,
        [License Expired] BIT DEFAULT 1,
        [Low Insulation Res] BIT DEFAULT 1,
        [Output Overcurrent] BIT DEFAULT 1,
        [Power Grid Failure] BIT DEFAULT 1,
        [PV String Backfeed] BIT DEFAULT 1,
        [Short circuit between phase to PE] BIT DEFAULT 1,
        [String Reversed] BIT DEFAULT 1,
        [Unbalanced Grid Voltage] BIT DEFAULT 1,
        [Upgrade Failed] BIT DEFAULT 1,
        [INVERTER CABINET TEMPERATURE] FLOAT,  
        [INVERTER DC ILLUSTRATION RESISTANCE] FLOAT
)
'''

cursor.execute(query)
conn.commit()
time.sleep(5)

# Create table current data
table_name = 'current'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        int_id VARCHAR(6) NOT NULL,
        cs_id VARCHAR(4) NOT NULL,
        [A PHASE CURRENT] FLOAT,
        [B PHASE CURRENT] FLOAT,
        [C PHASE CURRENT] FLOAT,
        [DC INPUT CURRENT] FLOAT,
        [PV1 INPUT CURRENT] FLOAT,
        [PV2 INPUT CURRENT] FLOAT,
        [PV3 INPUT CURRENT] FLOAT,
        [PV4 INPUT CURRENT] FLOAT,
        [PV5 INPUT CURRENT] FLOAT,
        [PV6 INPUT CURRENT] FLOAT,
        [PV7 INPUT CURRENT] FLOAT,
        [PV8 INPUT CURRENT] FLOAT,
        [PV9 INPUT CURRENT] FLOAT,
        [PV10 INPUT CURRENT] FLOAT,
        [PV11 INPUT CURRENT] FLOAT,
        [PV12 INPUT CURRENT] FLOAT,
        [RCD CURRENT] FLOAT  
)
'''

cursor.execute(query)
conn.commit()
time.sleep(5)

# Create table voltage data
table_name = 'voltage'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        int_id VARCHAR(6) NOT NULL,
        cs_id VARCHAR(4) NOT NULL,
        [AB LINE VOLTAGE] FLOAT,
        [BC LINE VOLTAGE] FLOAT,
        [CA LINE VOLTAGE] FLOAT,
        [DC INPUT VOLTAGE] FLOAT,
        [PV1 INPUT VOLTAGE] FLOAT,
        [PV2 INPUT VOLTAGE] FLOAT,
        [PV3 INPUT VOLTAGE] FLOAT,
        [PV4 INPUT VOLTAGE] FLOAT,
        [PV5 INPUT VOLTAGE] FLOAT,
        [PV6 INPUT VOLTAGE] FLOAT,
        [PV7 INPUT VOLTAGE] FLOAT,
        [PV8 INPUT VOLTAGE] FLOAT,
        [PV9 INPUT VOLTAGE] FLOAT,
        [PV10 INPUT VOLTAGE] FLOAT,
        [PV11 INPUT VOLTAGE] FLOAT,
        [PV12 INPUT VOLTAGE] FLOAT  
)
'''

cursor.execute(query)
conn.commit()
time.sleep(5)

# Create table power data
table_name = 'power'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        int_id VARCHAR(6) NOT NULL,
        cs_id VARCHAR(4) NOT NULL,
        [ACTIVE POWER VALUE] FLOAT,
        [DC INPUT POWER] FLOAT,
        [POWER FACTOR VALUE] FLOAT,
        [REACTIVE POWER VALUE] FLOAT 
)
'''

cursor.execute(query)
conn.commit()
time.sleep(5)

# Create table power data
table_name ='total'
query = f'''
    CREATE TABLE {table_name} (
        id INT IDENTITY(1,1),
        timestamp DATETIME,
        int_id VARCHAR(6) NOT NULL,
        cs_id VARCHAR(4) NOT NULL,
        [INVERTER EFFICIENCY] FLOAT,
        [INVERTER FREQUENCY VALUE] FLOAT,
        [INVERTER TOTAL CO2 REDUCTION] FLOAT,
        [INVERTER TOTAL ENERGY YIELD CURRENT DAY VALUE] FLOAT,
        [INVERTER TOTAL ENERGY YIELD CURRENT MONTH VALUE] FLOAT,
        [INVERTER TOTAL ENERGY YIELD CURRENT YEAR VALUE] FLOAT,
        [INVERTER TOTAL ENERGY YIELD VALUE] FLOAT   
)
'''

cursor.execute(query)
conn.commit()
time.sleep(5)

# Close the cursor and the connection
cursor.close()
conn.close() 