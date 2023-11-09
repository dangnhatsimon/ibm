# import libraries
import pyodbc 
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

for i in cs.keys():
    for j in range (1,cs[i]+1):
        table_name=f'INV{chr(i+64)}{j:02}'
        query = f'''
            CREATE TABLE {table_name} (
                id INT,
                timestamp DATETIME,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Abnormal Equipment] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Abnormal Ground] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Abnormal Leakage Current] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Abnormal Monitor Unit] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Abnormal String] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_AFCI Self-test Fault] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_COMUNICATION STATUS] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_DC Arc Fault] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Grid Frequency Instability] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Grid Overfrequency] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Grid Underfrequency] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Grid Overvoltage] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Grid Undervoltage] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_High String Voltage] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_High String Voltage to Ground] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_High Temperature] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER A PHASE CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER AB LINE VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER ACTIVE POWER VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER B PHASE CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER BC LINE VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER C PHASE CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER CA LINE VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER CABINET TEMPERATURE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER DC ILLUSTRATION RESISTANCE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER DC INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER DC INPUT POWER] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER DC INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER EFFICIENCY] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER FREQUENCY VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER LOCKED] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER POWER FACTOR VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV1 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV1 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV2 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV2 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV3 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV3 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV4 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV4 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV5 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV5 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV6 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV6 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV7 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV7 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV8 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV8 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV9 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV9 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV10 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV10 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV11 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV11 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV12 INPUT CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER PV12 INPUT VOLTAGE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER RCD CURRENT] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER REACTIVE POWER VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER STATUS] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER TOTAL CO2 REDUCTION] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER TOTAL ENERGY YIELD CURRENT DAY VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER TOTAL ENERGY YIELD CURRENT MONTH VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER TOTAL ENERGY YIELD CURRENT YEAR VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER TOTAL ENERGY YIELD VALUE] FLOAT,
                [CS{i:02}_INV{chr(i+64)}{j:02}_INVERTER WARNING] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Large DC of Output current] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_License Expired] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Low Insulation Res] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Output Overcurrent] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Power Grid Failure] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_PV String Backfeed] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Short circuit between phase to PE] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_String Reversed] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Unbalanced Grid Voltage] BIT DEFAULT 1,
                [CS{i:02}_INV{chr(i+64)}{j:02}_Upgrade Failed] BIT DEFAULT 1      
        )
        '''
        cursor.execute(query)
        conn.commit()

# Close the cursor and the connection
cursor.close()
conn.close() 