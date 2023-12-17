#!/bin/sh

## Write your code here to load the data from sales_data table in Mysql server to a sales.csv.
# mysql --host=127.0.0.1 --port=3306 --user=root --password=MzIyMDEtZGFuZ25o \
#         -e "USE sales;
#             SELECT * FROM sales_data \
#             INTO OUTFILE 'sales.csv' \
#             FIELDS ENCLOSED BY '\"' \
#             TERMINATED BY ';' \
#             ESCAPED BY '\"' \
#             LINES TERMINATED BY '\r\n';"

# mysql --host=127.0.0.1 --port=3306 --user=root --password=MzIyMDEtZGFuZ25o \
#         -e "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = 'sales_data';" \
#         | awk '{print $1}' | grep -iv ^COLUMN_NAME$ | \
#         tr '\n' ',' | sed 's/,$/\n/' > sales.csv
# mysqldump --user=root --password=MzIyMDEtZGFuZ25o \
#         --host=127.0.0.1 --port=3306 \
#         --tab="/home/project/" \
#         sales sales_data \
#         --fields-terminated-by=',' \
#         --fields-enclosed-by='"' \
#         --lines-terminated-by='\n' 
# sed "s/\"//g" sales_data.txt >> sales.csv

mysql --host=127.0.0.1 --port=3306 --user=root --password=MzIyMDEtZGFuZ25o -e "USE sales;\
        SELECT * FROM sales_data;" > sales.tsv
sed "s/\t/,/g" sales.tsv > sales.csv

## Select the data which is not more than 4 hours old from the current time.
cur_hour = $(date +%H)
mysql --host=127.0.0.1 --port=3306 --user=root --password=MzIyMDEtZGFuZ25o \
    -e "USE sales; SELECT * FROM sales_data WHERE $cur_hour - hour(timestamp) <=4"

export PGPASSWORD=MjcwODktZGFuZ25o;

psql --username=postgres --host=localhost --dbname=sales_new \
    -c "\COPY sales_data(rowid,product_id,customer_id,price,quantity,timestamp) \
    FROM '/home/project/sales.csv' delimiter ',' csv header;" 

 ## Delete sales.csv present in location /home/project
rm sales.csv
rm sales.tsv
 ## Write your code here to load the DimDate table from the data present in sales_data table

# psql --username=postgres --host=localhost --dbname=sales_new \
#     -c  "INSERT INTO DimDate(dateid,day,month,year)
#     SELECT dateid, \
#         DATE_PART('day', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')), \
#         DATE_PART('month',TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')), \
#         DATE_PART('year', TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')) \
#     FROM sales_data"

psql --username=postgres --host=localhost --dbname=sales_new \
    -c "INSERT INTO DimDate(dateid,day,month,year)
    SELECT dateid, \
        EXTRACT(DAY FROM timestamp) AS day, \
        DATE_PART('month',timestamp) AS month, \
        DATE_PART('year', timestamp) AS year \
    FROM sales_data"

## Write your code here to load the FactSales table from the data present in sales_data table

psql --username=postgres --host=localhost --dbname=sales_new \
    -c  "INSERT INTO FactSales(rowid,product_id,custome_id,price, total_price)
    SELECT rowid, product_id,customer_id,price,price*quantity
    FROM sales_data"

 ## Write your code here to export DimDate table to a csv

psql --username=postgres --host=localhost --dbname=sales_new \
    -c "\COPY DimDate TO '/home/project/DimDate.csv' WITH DELIMITER ',' CSV HEADER;"

 ## Write your code here to export FactSales table to a csv
 
psql --username=postgres --host=localhost --dbname=sales_new \
    -c "\COPY FactSales TO '/home/project/FactSales.csv' WITH DELIMITER ',' CSV HEADER;"