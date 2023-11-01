# Import libraries
import sqlite3
import pandas as pd

# Database initiation
conn = sqlite3.connect('STAFF.db')

table_name = 'Departments'
col_name = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
col_type={'DEPT_ID':'int', 'DEP_NAME':'str', 'MANAGER_ID':'int', 'LOC_ID':'str'}

# Reading the CSV file
csv_file = '/home/project/Departments.csv'
df = pd.read_csv(csv_file, names = col_name, dtype=col_type)

# Loading the data to a table
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print(f'Table {table_name} is ready')

# Append some data to the table
data_dict = {
    'DEPT_ID' : [9],
    'DEP_NAME' : ['Quality Assurance'],
    'MANAGER_ID' : ['30010'],
    'LOC_ID' : ['L0010']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)

# data_dict = {
#     'DEPT_ID' : 9,
#     'DEP_NAME' : 'Quality Assurance',
#     'MANAGER_ID' : '30010',
#     'LOC_ID' : 'L0010'
# }
# data_append = pd.DataFrame(data_dict,index=[0])

# View all entries
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# View only the department names
query_statement = f"SELECT DEP_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Count the total entries
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)