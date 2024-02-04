# DataTalks.club - Data Engineering Zoomcamp 2024 homework#2

### Setup

Follow the steps in this repo https://github.com/mage-ai/mage-zoomcamp beginning on **Let's get started**

``` bash
cd mage-zoomcamp
```
Make a copy of the `dev.env` file to `.env` in order to use this variables in the docker image

```bash
cp dev.env env
```
Then execute the commands in the previous repo

```bash
docker compose build
docker compose up -d
```

Now acces to your browser with this url http://localhost:6789 to enter Mage GUI

### Steps for loading data into Postgres

**Use Mage**

Go to file `io_config.yaml` and add a new section like `default` and on it's same label names `dev`.

Copy the `default`'s `Postgres` section into `dev` section.

Now replace all this parameters values with it's key like this to use the environment variable that holds the key:

```yaml
dev:
  # PostgresSQL
  POSTGRES_CONNECT_TIMEOUT: 10
  POSTGRES_DBNAME: "{{ env_var('POSTGRES_DBNAME') }}"
  POSTGRES_SCHEMA: "{{ env_var('POSTGRES_SCHEMA') }}"
  POSTGRES_USER: "{{ env_var('POSTGRES_USER') }}"
  POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
  POSTGRES_HOST: "{{ env_var('POSTGRES_HOST') }}"
  POSTGRES_PORT: "{{ env_var('POSTGRES_PORT') }}"
```

Then start with the process specified to load the data into `Postgres` and `GCP`.

## The questions answers

### Q1. Once the dataset is loaded, what's the shape of the data?

The answer was to this can be specied after all the dates where loaded 2020 and months 10, 11, 12. 
This code performed the data loading for all dates with `pd.concat`

```python
base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_---DATE---.csv.gz'

# columns types
columns_dtypes = {
    'VendorID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'RatecodeID': pd.Int64Dtype(),
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': float,
    'trip_type': float,
    'congestion_surcharge': float,
}
# the date columns
date_time_columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

# the data frame that will hold all the csv files to load
final_df = pd.DataFrame()

# dates of files to load
data_dates = ['2020-10', '2020-11', '2020-12']

# load the files and pd.concat to merge in final_df
for d in data_dates:
    url = base_url.replace('---DATE---', d)
    loaded_data = pd.read_csv(
        url, 
        sep=',',
        compression='gzip',
        dtype=columns_dtypes,
        parse_dates=date_time_columns,
    )
    print(f'Loaded: {d} with {len(loaded_data)}')
    ## join all data
    final_df = pd.concat([final_df, loaded_data], ignore_index=True)
```

### Q2. Upon filtering the dataset where the passenger count is greater than 0 and the trip distance is greater than zero, how many rows are left?

To answer this had to make this transformation and check the data frame size at the end:

```python
# filter data
data = data[data["passenger_count"] > 0]
data = data[data["trip_distance"] > 0]
data = data[data["vendor_id"] > 0]
```
### Q3. Which of the following creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date?

The right statement was `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`

### Q4. What are the existing values of VendorID in the dataset?

This can be found with many ways, but I found this simple and helpful:

```python
data['vendor_id'].unique()
```

### Q5. How many columns need to be renamed to snake case?

I think this code explains it

```python 
# rename columns
column_rename_map = {
    'VendorID': 'vendor_id',
    'RatecodeID': 'rate_code_id',
    'PULocationID': 'pu_location_id',
    'DOLocationID': 'do_location_id',
}
```
### Q6. Once exported, how many partitions (folders) are present in Google Cloud?

For the answer made a `Data Exporter` in `Mage` web app that execute the data loading and a transformation.

This is the code for the exporter:

```python 
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/<my creds file>'

project_id = '<my project id>'
bucket_name = '<my bucket name in gcs>'
table_name = 'green_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```

Then checked the result in my bucket folder with the `bucket_name` used in the previous script.
And at the bottom-left corner got the number for this question. 