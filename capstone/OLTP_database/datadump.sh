#!/bin/bash

# Initial connection parameters
USERNAME="root"    
PASSWORD="MTU0MDMtZGFuZ25o"
DATABASE="sales"
TABLE="sales_data"
# Output file
sql_file="sales_data.sql"

# Export data
mysqldump -u $USERNAME -p$PASSWORD $DATABASE $TABLE > $sql_file

# Check if mysqldump was successful
if [ $? -eq 0 ]; then
  echo "Data export successful. Check $sql_file for the exported data."
else
  echo "Error: Data export failed."
fi
