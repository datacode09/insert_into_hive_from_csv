#!/bin/bash

# Set the name of the database
database="dev"

# Set the name of the table
table="my_table"

# Set the location of the table data
location="/path/to/table/data"

# Set the name of the partition column
partition_column="datetime"

# Set the partition format (YYYY-MM-DD)
partition_format="%Y-%m-%d"

# Set the path to the CSV file on the edge node
csv_file="/path/to/csv/file.csv"

# Set the delimiter used in the CSV file
delimiter=","

# Set the DDL statement for the table
ddl=$(cat <<EOF
CREATE TABLE ${database}.${table} (
  id INT,
  name STRING,
  age INT,
  email STRING,
  address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>,
  salary DOUBLE,
  ${partition_column} TIMESTAMP
)
PARTITIONED BY (${partition_column} STRING)
STORED AS ORC
LOCATION '${location}';
EOF
)

# Execute the Hive command to create the table
hive -e "${ddl}"

# Create partitions for the past 30 days
for i in {0..29}; do
  partition_date=$(date -d "-$i days" +"${partition_format}")
  partition_location="${location}/${partition_column}=${partition_date}"
  alter_partition_ddl=$(cat <<EOF
  ALTER TABLE ${database}.${table}
  ADD PARTITION (${partition_column}='${partition_date}')
  LOCATION '${partition_location}';
EOF
  )
  # Execute the Hive command to create the partition
  hive -e "${alter_partition_ddl}"
done

# Load data into the table
load_data_ddl=$(cat <<EOF
LOAD DATA LOCAL INPATH '${csv_file}'
OVERWRITE INTO TABLE ${database}.${table}
PARTITION (${partition_column})
FIELDS TERMINATED BY '${delimiter}';
EOF
)

# Execute the Hive command to load the data
hive -e "${load_data_ddl}"
