#!/bin/bash

# Set the MySQL username and password
MYSQL_USER="root"
MYSQL_PASSWORD="MjM0NTgtZGFuZ25o"

# Get the current date in the YYYYMMDD format
CURRENT_DATE=$(date +%Y%m%d)

# Define the backup directory path
BACKUP_DIR="/tmp/mysqldumps/$CURRENT_DATE"

# Create the backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Backup all databases using mysqldump
mysqldump --all-databases --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" > "$BACKUP_DIR/all-databases-backup.sql"

# Check if the mysqldump command was successful
if [ $? -eq 0 ]; then
  echo "MySQL backup completed successfully."
else
  echo "MySQL backup failed."
fi
