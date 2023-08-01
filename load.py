
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET")

CSV_FOLDER = 'csv/'
PARQUET_FOLDER = 'parquet/'

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def get_filenames(year, service):
    filenames = []
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"    
        filenames.append({"file_name":file_name, "year":year, "month":month, "service":service})
    return filenames

def get_local_files(file_info_list):
    for file_info in file_info_list:
        file_name = file_info['file_name'] 
        request_url = f"{init_url}{file_info['service']}/{file_name}"
        r = requests.get(request_url)
        open(f'./{CSV_FOLDER}{file_name}', 'wb').write(r.content)
        print(f"Local: {file_name}")

def create_parqet_files(file_info_list):
    for file_info in file_info_list:
        file_name = file_info['file_name']
        service = file_info['service']

        dtypes = None
        date_cols = None
        if service == 'yellow':
            date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
            dtypes = {
                'VendorID': 'Int64',
                'passenger_count': 'Int64',
                'trip_distance': 'Float64',
                'RatecodeID': 'Int64',
                'store_and_fwd_flag': 'str',
                'PULocationID': 'Int64',
                'DOLocationID': 'Int64',
                'payment_type': 'Int64',
                'fare_amount': 'Float64',
                'extra': 'Float64',
                'mta_tax': 'Float64',
                'tip_amount': 'Float64',
                'tolls_amount': 'Float64',
                'improvement_surcharge': 'Float64',
                'total_amount': 'Float64',
                'congestion_surcharge': 'Float64',
            }
        elif service == 'green':
            date_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
            dtypes = {
                'VendorID': 'Int64',
                'store_and_fwd_flag': 'str',
                'RatecodeID': 'Int64',
                'PULocationID': 'Int64',
                'DOLocationID': 'Int64',
                'passenger_count': 'Int64',
                'trip_distance': 'Float64',
                'fare_amount': 'Float64',
                'extra': 'Float64',
                'mta_tax': 'Float64',
                'tip_amount': 'Float64',
                'tolls_amount': 'Float64',
                'ehail_fee': 'Float64',
                'improvement_surcharge': 'Float64',
                'total_amount': 'Float64',                
                'payment_type': 'Int64',
                'trip_type': 'Int64',
                'congestion_surcharge': 'Float64'
            }
        else:
            date_cols = ['pickup_datetime', 'dropOff_datetime']
            dtypes = {
                'dispatching_base_num': 'str',
                'PUlocationID': 'Int64',
                'DOlocationID': 'Int64',
                'SR_Flag': 'str',
                'Affiliated_base_number': 'str',
            }
        
        # read it back into a parquet file
        df = pd.read_csv(f'./{CSV_FOLDER}{file_name}', compression='gzip', low_memory=False, dtype=dtypes, parse_dates=date_cols)

        df.rename(columns=lambda x: x.lower(), inplace=True)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(f'./{PARQUET_FOLDER}{file_name}', engine='pyarrow')
        print(f"Parquet: {file_name}")

def send_to_gcs(file_info_list):
    
    for file_info in file_info_list:
        file_name = file_info['file_name']
        service = file_info['service']

        # upload it to gcs 
        file_name = file_name.replace('.csv.gz', '.parquet')
        upload_to_gcs(BUCKET, f"trip_data/{service}/{file_name}", f'./{PARQUET_FOLDER}{file_name}')
        print(f"GCS: {service}/{file_name}")


file_info_list = []
file_info_list.extend(get_filenames('2019', 'fhv'))
file_info_list.extend(get_filenames('2019', 'yellow'))
file_info_list.extend(get_filenames('2019', 'green'))
file_info_list.extend(get_filenames('2020', 'green'))
file_info_list.extend(get_filenames('2020', 'yellow'))

get_local_files(file_info_list)
create_parqet_files(file_info_list)
send_to_gcs(file_info_list)
