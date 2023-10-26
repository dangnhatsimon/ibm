# cp-access-log.sh
# This script downloads the file 'web-server-access-log.txt.gz'
# from "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/".

# The script then extracts the .txt file using gunzip.

# The .txt file contains the timestamp, latitude, longitude 
# and visitor id apart from other data.

# Transforms the text delimeter from "#" to "," and saves to a csv file.
# Loads the data from the CSV file into the table 'access_log' in PostgreSQL database.

# Download the access log file.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/

# Unzip the gzip file.
gunzip -k web-server-access-log.txt.gz

# Extract required fields from the file. Redirect the extracted output into a file.
cut -d"#" -f1-4 web-server-access-log.txt > extracted-data-access.txt

# Transform the data into CSV format.
tr "#" "," < extracted-data-access.txt > access_log.csv

# Load the data into the table access_log in PostgreSQL
echo "\c template1;\COPY access_log FROM '/home/project/access_log.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost