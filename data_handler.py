import requests,json,csv, os, datetime

from google.cloud import storage,bigquery

# Method to upload to google Buckets
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(
        'weather-dwh-storage.json')
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

# Format json for DB injestion
def format_json_for_db_injestion(json_list,output_filename):
    os.remove(output_filename)
    with open(output_filename, "w") as f:
        f.write('\n'.join(json.dumps(i).replace('"3h"', '"_3h_"') for i in json_list))

def retrieve_weather_per_api(api, city):
    city_forecasts = []
    response = requests.get(
        'http://api.openweathermap.org/data/2.5/'+ api +'?q=' + city + '&APPID=78b0bc366c6e99bf271709c77d07ce7e')
    print(response)
    response_data = json.loads(response.text)

    if api == 'forecast':
        for item in response_data['list']:
            item.update(response_data['city'])
            city_forecasts.append(item)
        return city_forecasts
    elif api == 'weather':
        return response_data


def upload_to_gbq(dataset, json_url, table_name):

    load_start = datetime.datetime.now()
    client = bigquery.Client.from_service_account_json(
        'weather-dwh-gbq_storage.json')
    dataset_id = dataset

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    uri = json_url

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_name),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table_name))
    print("Loaded {} rows.".format(destination_table.num_rows))
    load_end = datetime.datetime.now()
    print('Load Duration: ' + str(load_end-load_start))


def update_hourly_weather_stats():
    # Perform a query.
    client = bigquery.Client.from_service_account_json(
        'weather-dwh-gbq_storage.json')
    QUERY = (
        'INSERT INTO `weather-dwh.hourly_weather.stats` AS SELECT *, now() AS api_pulled FROM `weather-dwh.ods_30days.curr_weather`'
        'WHERE api_pulled > (select max(api_pulled) from `weather-dwh.hourly_forecasts.forecasts*`)')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        print(row.name)

# Prep
cities_forecasts = []
weather_datas = []

for city in ['Bradford,gb','Southampton,gb','Oxford,gb','Armagh,gb','Aberporth,gb' ]:
    weather_datas.append(retrieve_weather_per_api('weather', city))
    cities_forecasts.extend(retrieve_weather_per_api('forecast', city))
upload_blob('weather-dwh', 'curr_weather.json', 'curr_weather.json')

# Create files, upload to store and DB
format_json_for_db_injestion(cities_forecasts, "forecasts.json")
upload_blob('weather-dwh', 'forecasts.json', 'forecasts.json')

# Create files, upload to store and DB
format_json_for_db_injestion(weather_datas, "curr_weather.json")

upload_to_gbq('ods_7days', 'gs://weather-dwh/forecasts.json', 'forecasts')
upload_to_gbq('ods_30days', 'gs://weather-dwh/curr_weather.json', 'curr_weather')

update_hourly_weather_stats()
