import requests,json,csv, os, datetime

from google.cloud import storage,bigquery

batch_time = str(datetime.datetime.now())

def retrieve_weather_per_api(endpoint, city):
    weather_list = []
    response = requests.get(
        'http://api.openweathermap.org/data/2.5/' + endpoint + '?q=' + city + '&APPID=78b0bc366c6e99bf271709c77d07ce7e')
    print(response)
    response_data = json.loads(response.text)

    if endpoint == 'forecast':
        for item in response_data['list']:
            item.update(response_data['city'])
            item.update({'batch_time' : batch_time})
            weather_list.append(item)

    elif endpoint == 'weather':
        response_data.update({'batch_time' : batch_time})
        return response_data

    return weather_list

# Format json for DB injestion
def format_json_for_db_injestion(json_list,output_filename):
    # os.remove(output_filename)
    with open(output_filename, "w") as f:
        f.write('\n'.join(json.dumps(i).replace('"3h"', '"_3h_"') for i in json_list))

# Upload to Google Bucket
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

# Upload to GBQ
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
    print('Load Duration: ' + str(load_end-load_start) + '\n' + str(load_end))

# DB Operations
def update_hourly_weather_stats():
    # Perform a query.
    client = bigquery.Client.from_service_account_json(
        'weather-dwh-gbq_storage.json')
    QUERY = """
        #standardSQL
        INSERT INTO `weather-dwh.hourly_weather.stats` 
        SELECT batch_time, cod, name, id, timezone, sys, clouds, dt, base, wind.deg wind_deg, wind.speed wind_speed, coord, coord.lat, coord.lon, visibility, main, weather  
        FROM `weather-dwh.ods_30days.curr_weather` WHERE batch_time > (select max(batch_time) from `weather-dwh.hourly_weather.stats*`)"""

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        print(row.name)

def main():

    # Prep
    forecasts = []
    curr_weather = []

    # Get api response as append to lists
    for city in ['Bradford,gb','Southampton,gb']:##,'Oxford,gb','Armagh,gb','Aberporth,gb' ]:
        curr_weather.append(retrieve_weather_per_api('weather', city))
        forecasts.extend(retrieve_weather_per_api('forecast', city))

    # Create files, upload to store and DB
    format_json_for_db_injestion(curr_weather, "curr_weather.json")
    upload_blob('weather-dwh', 'curr_weather.json', 'curr_weather.json')

    # Create files, upload to store and DB
    format_json_for_db_injestion(forecasts, "forecasts.json")
    upload_blob('weather-dwh', 'forecasts.json', 'forecasts.json')

    upload_to_gbq('ods_30days', 'gs://weather-dwh/curr_weather.json', 'curr_weather')
    upload_to_gbq('ods_7days', 'gs://weather-dwh/forecasts.json', 'forecasts')


    update_hourly_weather_stats()

if __name__ == '__main__':
    main()