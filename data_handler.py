import requests,json,csv, datetime

# from google.cloud import storage
#
# def upload_to_bucket(blob_name, path_to_file, bucket_name):
#     """ Upload data to a bucket"""
#
#     # Explicitly use service account credentials by specifying the private key
#     # file.
#     storage_client = storage.Client.from_service_account_json(
#         'creds.json')
#
#     #print(buckets = list(storage_client.list_buckets())
#
#     bucket = storage_client.get_bucket(bucket_name)
#     blob = bucket.blob(blob_name)
#     blob.upload_from_filename(path_to_file)
#
#     #returns a public url
#     return blob.public_url

def json_formatter(json_list,output_filename):
    with open(output_filename, "w") as f:
        f.write('\n'.join(json.dumps(i).replace('"3h"', '"_3h_"') for i in json_list))

response_lists = []

pulltime = str(datetime.datetime.now())

for city in ['Bradford,gb','Southampton,gb','Oxford,gb','Armagh,gb','Aberporth,gb' ]:

    response = requests.get('http://api.openweathermap.org/data/2.5/forecast?q='+city+'&APPID=78b0bc366c6e99bf271709c77d07ce7e')
    print(response)
    json_data = json.loads(response.text)

    for item in json_data['list']:
        item.update(json_data['city'])
        item.update({'api_pulled' : pulltime})
        response_lists.append(item)




