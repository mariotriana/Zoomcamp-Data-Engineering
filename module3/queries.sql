  -- Creating EXTERNAL TABLE referring TO gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `weighty-elf-412403.demo_dataset.external_green_tripdata` OPTIONS ( format = 'PARQUET',
    uris = ['gs://weighty-elf-412403-terra-bucket/data/green_tripdata_2022-*.parquet'] );
    
-- Check green trip data
SELECT * FROM weighty-elf-412403.demo_dataset.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned AS
SELECT * FROM weighty-elf-412403.demo_dataset.external_green_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE weighty-elf-412403.demo_dataset.green_tripdata_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM weighty-elf-412403.demo_dataset.external_green_tripdata;

--counts
SELECT count(*) as trips
FROM weighty-elf-412403.demo_dataset.external_green_tripdata
WHERE PULocationID IS NOT NULL;

SELECT count(*) as trips
FROM weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned
WHERE PULocationID IS NOT NULL; 

SELECT count(*) as trips
FROM weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned
WHERE fare_amount=0; 

-- Impact of partition
SELECT DISTINCT(PULocationID)
FROM weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID)
FROM weighty-elf-412403.demo_dataset.green_tripdata_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2012-06-01' AND '2022-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `demo_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE weighty-elf-412403.demo_dataset.green_tripdata_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM weighty-elf-412403.demo_dataset.external_green_tripdata;

--data of materialized table
SELECT DISTINCT(PULocationID)
FROM weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--data of clustered table
SELECT DISTINCT(PULocationID)
FROM weighty-elf-412403.demo_dataset.green_tripdata_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--bonus
SELECT count(*) as bonus
FROM weighty-elf-412403.demo_dataset.green_tripdata_non_partitioned