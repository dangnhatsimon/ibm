#!/bin/bash
# The above line tells the interpreter this code needs to be run as a shell script.
# This will be printed on to the screen. In the case of cron job, it will be printed to the logs.
echo "Pulling Database: This may take a few minutes"

# Set user and password
mysql_user='root'
mysql_password='Mjk3ODktZGFuZ25o'

# Number of days to store the backup 
keep_day=30

# Set the folder where the database backup will be stored
backupfolder=/tmp/mysqldumps/$(date +%Y%m%d)

# Create the backup folder
if [ ! -d $backupfolder ]; then
  mkdir -p $backupfolder
fi

# Delete old file
function delete_old_backups() {
  # echo "Deleting $backupfolder/*.gz older than $keep_day days"
  find $backupfolder -type f -name "*.gz" -mtime +$keep_day -delete
}

# Get database list need to be saved, excludes: information_schema,performance_schema, mysql, sys
function database_list() {
    echo "Geting list of databases from MySQL server"
    databases=$(mysql -u"$mysql_user" -p"$mysql_password" -e "SHOW DATABASES;" | grep -Ev "(Database|information_schema|performance_schema|mysql|sys)")
}

# Create a backup
database_list
for database in $databases; do
    echo "Backing up database: $database"
    mysqldump --user=$mysql_user --password=$mysql_password $database > "$backupfolder/$database.sql"
    if [ $? -eq 0 ]; then
    echo 'Sql dump created'
        # Compress backup 
        gzip -c "$backupfolder/$database.sql" > "$backupfolder/$database.sql.gz"
        if [ $? -eq 0 ]; then
            echo 'The backup was successfully compressed'
        else
            echo 'Error compressing backupBackup was not created!' 
            exit 1
        fi 
    else
        echo 'pg_dump return non-zero code No backup was created!' 
        exit 1
    fi
done

delete_old_backups