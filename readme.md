# Zoomcamp Date Engineer Course Script to load and clean required files to GC storage

## The original script was from here

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse/extras

This script has been modified to clean up the data types and rename the fields as lowercase before uploading them as parquet files.  It has also been modified to separate the extract (download), transformation, and load into separate routines to allow them to be run independently (mainly to avoid redownloading files as I was debugging)
