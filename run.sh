#!/bin/bash

# Set Hive database and table name
DATABASE_NAME="your_database_name"
TABLE_NAME="your_table_name"

# Set path to input CSV file
CSV_FILE_PATH="/path/to/your_csv_file.csv"

# Run Hive query to create a temporary table and load CSV data
hive -e "USE $DATABASE_NAME; \
    CREATE TABLE temp_table \
    (column1 STRING, column2 INT, column3 DOUBLE) \
    ROW FORMAT DELIMITED \
    FIELDS TERMINATED BY ',' \
    LINES TERMINATED BY '\n' \
    STORED AS TEXTFILE; \
    LOAD DATA LOCAL INPATH '$CSV_FILE_PATH' OVERWRITE INTO TABLE temp_table;"

# Run Hive query to insert data from temporary table into target table
hive -e "USE $DATABASE_NAME; \
    INSERT INTO TABLE $TABLE_NAME \
    SELECT column1, column2, column3 FROM temp_table;"

# Clean up temporary table
hive -e "USE $DATABASE_NAME; \
    DROP TABLE temp_table;"
