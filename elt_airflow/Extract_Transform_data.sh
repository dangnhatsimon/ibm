#!/bin/bash

# download the dataset
wget -P /home/project/airflow/dags https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz

# unzip the data
tolldata="/home/project/airflow/dags/tolldata.tgz"
tar -xvzf $tolldata -C $AIRFLOW_HOME/dags/

# extract data from csv file
in_csv_data="$AIRFLOW_HOME/dags/vehicle-data.csv"
out_csv_data="$AIRFLOW_HOME/dags/csv_data.csv"
echo "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type" > $out_csv_data
cat $in_csv_data | cut -d ',' -f1-4 >> $out_csv_data

# extract data from tsv file
in_tsv_data="$AIRFLOW_HOME/dags/tollplaza-data.tsv"
out_tsv_data="$AIRFLOW_HOME/dags/tsv_data.csv"
echo "Number of axles,Tollplaza id,Tollplaza code" > $out_tsv_data
awk -F'\t' '{ print $5","$6","$7; }' $in_tsv_data >> $out_tsv_data
# cut -d$'\t' -f5-7 $in_tsv_data | tr '\t' ',' >> $out_tsv_data

# extract data from fixed width file
in_fwf_data="$AIRFLOW_HOME/dags/payment-data.txt"
out_fwf_data="$AIRFLOW_HOME/dags/fixed_width_data.csv"
echo "Type of Payment code,Vehicle Code" > $out_fwf_data
awk -F' ' '{ print $10","$11; }' $in_fwf_data >> $out_fwf_data

# extracted from previous task
extracted_data="$AIRFLOW_HOME/dags/extracted_data.csv"
paste -d ',' $out_csv_data $out_tsv_data $out_fwf_data > $extracted_data

# transform and load the data
transformed_data="$AIRFLOW_HOME/dags/transformed_data.csv"
awk -F',' '{ if (NR!=1) { print $1","$2","$3","toupper($4)","$5","$6","$7","$8","$9; } }' < $extracted_data > $transformed_data

cp Extract_Transform_data.sh $AIRFLOW_HOME/dags