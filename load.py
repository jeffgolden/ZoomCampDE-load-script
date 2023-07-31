import io
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


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        dtypes = None
        date_cols = None
        if service == 'yellow':
            date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
            dtypes = {
                'VendorID': 'Int64',
                'paseenger_count': 'Int64',
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
                'congestion_surcharge': 'Float64'
            }
        elif service == 'green':
            date_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
            dtypes = {
                'VendorID': 'Int64',
                'paseenger_count': 'Int64',
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
                'congestion_surcharge': 'Float64'
            }
        else:
            date_cols = ['pickup_datetime', 'dropOff_datetime']
            dtypes = {
                'dispatching_base_num': 'str',
                'PULocationID': 'Int64',
                'DOlocationID': 'Int64',
                'SR_Flag': 'str',
                'Affiliated_base_number': 'str',
            }

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip', low_memory=False, dtype=dtypes, parse_dates=date_cols)
        df.rename(columns=lambda x: x.lower(), inplace=True)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"data_copy/{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')