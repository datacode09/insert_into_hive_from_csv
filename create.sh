#!/bin/bash

# Set the name of the database
database="dev"

# Set the name of the table
table="my_table"

# Set the location of the table data
location="/path/to/table/data"

# Set the schema of the table
schema="(column1 STRING, column2 INT, column3 DOUBLE)"

# Execute the Hive command to create the table
hive -e "CREATE TABLE ${database}.${table} ${schema} LOCATION '${location}';"
