# Set the path to the CSV file on the edge node
csv_file="/path/to/csv/file.csv"

# Set the delimiter used in the CSV file
delimiter=","

# Set the name of the partition column
partition_column="datetime"

# Set the partition format (YYYY-MM-DD)
partition_format="%Y-%m-%d"

# Set the number of mappers to use for parallel processing
num_mappers=4

# Load data into the table
load_data_ddl=$(cat <<EOF
SET mapred.map.tasks=${num_mappers};
INSERT INTO ${database}.${table} PARTITION (${partition_column})
SELECT
  id,
  name,
  age,
  email,
  address,
  salary,
  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(${partition_column}, '${partition_format}')) AS TIMESTAMP) AS ${partition_column}
FROM
  (
    SELECT
      split(line, '${delimiter}')
    FROM
      (SELECT get_file_content('${csv_file}') AS line) t1
  ) t2
WHERE
  size(t2) = 7;
EOF
)

# Execute the Hive command to load the data
hive -e "${load_data_ddl}"
