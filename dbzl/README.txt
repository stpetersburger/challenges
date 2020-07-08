The test assignment for the data engineer within the OLX group

1. Create an instance of REDSHIFT in AWS
2. Create table to allocate data from the S3
3. Create script to get the data from s3 to the table, created in the newly redshift instance
4. Get the data (COPY...)
5. Create Tables as per assignment, with the optimization (DISTKEY,SORTKEY, fields compression)
6. Elaborate the logic for calculations
7. Apply the logic via SQL queries

NB! Nested queries, due to simplicity, were not created with the following:

WITH table_name AS (sql query)
SELECT something FROM table_name;

OR

DROP IF EXISTS table_name;
CREATE TEMPORARU TABLE table_name AS ...
